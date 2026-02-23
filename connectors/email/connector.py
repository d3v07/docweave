"""IMAP Email connector for ingesting emails."""
import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import hashlib
from datetime import datetime
from typing import AsyncIterator, Optional, List
from dataclasses import dataclass, field

from connectors.base import BaseConnector, ConnectorConfig
from shared.models.document import DocumentEventModel as DocumentEvent, EventType


@dataclass
class EmailConfig(ConnectorConfig):
    """Configuration for email connector."""
    imap_server: str = "imap.gmail.com"
    imap_port: int = 993
    username: str = ""
    password: str = ""  # App-specific password recommended
    folders: List[str] = field(default_factory=lambda: ["INBOX"])
    use_ssl: bool = True
    mark_as_read: bool = False
    max_emails_per_poll: int = 50
    since_date: Optional[str] = None  # IMAP date format: "01-Jan-2024"


@dataclass
class EmailMessage:
    """Parsed email message."""
    message_id: str
    subject: str
    sender: str
    recipients: List[str]
    date: datetime
    body_text: str
    body_html: Optional[str]
    attachments: List[dict]
    headers: dict
    raw_content: bytes


class EmailConnector(BaseConnector):
    """Connector for ingesting emails via IMAP."""

    def __init__(self, config: EmailConfig):
        super().__init__(config)
        self.config: EmailConfig = config
        self._connection: Optional[imaplib.IMAP4_SSL] = None
        self._processed_ids: set = set()

    async def connect(self) -> None:
        """Connect to IMAP server."""
        try:
            if self.config.use_ssl:
                self._connection = imaplib.IMAP4_SSL(
                    self.config.imap_server,
                    self.config.imap_port
                )
            else:
                self._connection = imaplib.IMAP4(
                    self.config.imap_server,
                    self.config.imap_port
                )

            self._connection.login(self.config.username, self.config.password)
            self._connected = True
        except Exception as e:
            self._connected = False
            raise ConnectionError(f"Failed to connect to IMAP server: {e}")

    async def disconnect(self) -> None:
        """Disconnect from IMAP server."""
        if self._connection:
            try:
                self._connection.logout()
            except Exception:
                pass
        self._connection = None
        self._connected = False

    async def poll_changes(self) -> AsyncIterator[DocumentEvent]:
        """Poll for new emails."""
        if not self._connection:
            raise ConnectionError("Not connected to IMAP server")

        for folder in self.config.folders:
            try:
                self._connection.select(folder)

                # Build search criteria
                search_criteria = "UNSEEN"
                if self.config.since_date:
                    search_criteria = f"(UNSEEN SINCE {self.config.since_date})"

                _, message_numbers = self._connection.search(None, search_criteria)

                if not message_numbers[0]:
                    continue

                msg_nums = message_numbers[0].split()[:self.config.max_emails_per_poll]

                for num in msg_nums:
                    _, msg_data = self._connection.fetch(num, "(RFC822)")

                    if not msg_data or not msg_data[0]:
                        continue

                    raw_email = msg_data[0][1]
                    email_msg = self._parse_email(raw_email)

                    if not email_msg or email_msg.message_id in self._processed_ids:
                        continue

                    self._processed_ids.add(email_msg.message_id)

                    # Generate document ID from message ID
                    doc_id = f"email_{hashlib.md5(email_msg.message_id.encode()).hexdigest()[:12]}"

                    yield DocumentEvent(
                        event_type=EventType.CREATED,
                        document_id=doc_id,
                        timestamp=datetime.utcnow(),
                        source_connector=self.config.name,
                        metadata={
                            "message_id": email_msg.message_id,
                            "subject": email_msg.subject,
                            "sender": email_msg.sender,
                            "date": email_msg.date.isoformat() if email_msg.date else None,
                            "folder": folder,
                            "has_attachments": len(email_msg.attachments) > 0
                        }
                    )

                    # Optionally mark as read
                    if self.config.mark_as_read:
                        self._connection.store(num, "+FLAGS", "\\Seen")

            except Exception as e:
                # Log error but continue with other folders
                print(f"Error processing folder {folder}: {e}")
                continue

        self._last_poll = datetime.utcnow()

    def _parse_email(self, raw_email: bytes) -> Optional[EmailMessage]:
        """Parse raw email bytes into EmailMessage."""
        try:
            msg = email.message_from_bytes(raw_email)

            # Extract message ID
            message_id = msg.get("Message-ID", "")

            # Decode subject
            subject = ""
            raw_subject = msg.get("Subject", "")
            if raw_subject:
                decoded_parts = decode_header(raw_subject)
                subject_parts = []
                for part, encoding in decoded_parts:
                    if isinstance(part, bytes):
                        subject_parts.append(part.decode(encoding or "utf-8", errors="replace"))
                    else:
                        subject_parts.append(part)
                subject = "".join(subject_parts)

            # Extract sender
            sender = msg.get("From", "")

            # Extract recipients
            recipients = []
            for header in ["To", "Cc"]:
                if msg.get(header):
                    recipients.extend(msg.get(header).split(","))

            # Parse date
            date = None
            date_str = msg.get("Date")
            if date_str:
                try:
                    date = parsedate_to_datetime(date_str)
                except Exception:
                    date = datetime.utcnow()

            # Extract body
            body_text = ""
            body_html = None
            attachments = []

            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition", ""))

                    if "attachment" in content_disposition:
                        # Extract attachment info
                        filename = part.get_filename() or "attachment"
                        attachments.append({
                            "filename": filename,
                            "content_type": content_type,
                            "size": len(part.get_payload(decode=True) or b"")
                        })
                    elif content_type == "text/plain":
                        payload = part.get_payload(decode=True)
                        if payload:
                            body_text = payload.decode("utf-8", errors="replace")
                    elif content_type == "text/html":
                        payload = part.get_payload(decode=True)
                        if payload:
                            body_html = payload.decode("utf-8", errors="replace")
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body_text = payload.decode("utf-8", errors="replace")

            # Extract headers
            headers = {k: v for k, v in msg.items()}

            return EmailMessage(
                message_id=message_id,
                subject=subject,
                sender=sender,
                recipients=recipients,
                date=date,
                body_text=body_text,
                body_html=body_html,
                attachments=attachments,
                headers=headers,
                raw_content=raw_email
            )

        except Exception as e:
            print(f"Error parsing email: {e}")
            return None

    async def fetch_document(self, doc_id: str) -> dict:
        """Fetch email content by document ID."""
        # In a full implementation, we'd store message references
        # For now, return a placeholder
        return {
            "id": doc_id,
            "content_type": "message/rfc822",
            "error": "Direct fetch not implemented - emails are processed during poll"
        }

    async def health_check(self) -> bool:
        """Check IMAP connection health."""
        if not self._connection:
            return False
        try:
            self._connection.noop()
            return True
        except Exception:
            return False
