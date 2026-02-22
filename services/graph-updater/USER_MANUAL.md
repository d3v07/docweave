# DocWeave Graph Updater - Complete Beginner's Guide

## Part 1: What Is This Thing?

### The Problem We're Solving

Imagine you're a researcher collecting facts from thousands of documents. You read:
- Document A says: "Tesla was founded in 2003"
- Document B says: "Tesla was founded in 2004"
- Document C says: "Tesla's CEO is Elon Musk"
- Document D says: "Tesla's CEO is the guy who owns Twitter"

Now you have problems:
1. **Conflicting information** - Which founding year is correct?
2. **Same thing, different words** - "Elon Musk" and "the guy who owns Twitter" are the same person
3. **Tracking sources** - Which document said what?
4. **Finding truth** - What's the most reliable answer?

**The Graph Updater solves all of this.**

### What It Actually Does

Think of it as a **smart filing cabinet** that:

1. **Stores facts as connections** (not just text)
   ```
   [Tesla] --founded_in--> [2003]
   [Tesla] --has_ceo--> [Elon Musk]
   ```

2. **Detects conflicts** automatically
   ```
   ⚠️ CONFLICT: Tesla founded_in 2003 vs 2004
   ```

3. **Tracks where facts came from**
   ```
   "founded in 2003" ← Annual Report (95% confidence)
   "founded in 2004" ← Wikipedia (70% confidence)
   ```

4. **Decides what's true** using rules you control

### The Components (Like Parts of a Car)

| Component | What It Is | Car Analogy |
|-----------|-----------|-------------|
| **Neo4j** | Database that stores everything | The fuel tank - holds all the data |
| **Kafka** | Message queue for processing | The transmission - moves data through the system |
| **API Server** | The brain that does the work | The engine - makes everything run |
| **Dashboard** | Visual interface for humans | The steering wheel & dashboard - how you control it |

---

## Part 2: Setting Up (Starting the Car)

### What You Need First

Before we begin, make sure you have these installed:

1. **Docker Desktop** - Download from https://docker.com
   - This runs all our services in containers (like apps on your phone)
   - After installing, make sure Docker is running (you'll see a whale icon in your menu bar)

2. **Node.js** - Download from https://nodejs.org (get the LTS version)
   - This runs the dashboard website
   - To check if it's installed, open Terminal and type: `node --version`

### Step 1: Open Terminal and Navigate

```bash
# Open Terminal (Mac: Cmd+Space, type "Terminal")
# Navigate to the project folder

cd /Users/dev/Documents/PROJECT_LOWLEVEL/DocWeave/services/graph-updater
```

**What you should see:** Your terminal prompt changes to show you're in that folder.

### Step 2: Start the Backend Services

```bash
docker-compose up -d
```

**What this does:**
- Downloads and starts Neo4j (the database)
- Downloads and starts Kafka (the message system)
- Starts the API server
- Creates all the connections between them

**What you should see:**
```
[+] Running 6/6
 ✔ Network graph-updater-network  Created
 ✔ Container graph-updater-neo4j  Started
 ✔ Container graph-updater-zookeeper  Started
 ✔ Container graph-updater-kafka  Started
 ✔ Container graph-updater-kafka-init  Started
 ✔ Container graph-updater-dev  Started
```

**Wait 30-60 seconds** for everything to fully start. The services need time to initialize.

### Step 3: Check If Everything Is Running

```bash
docker-compose ps
```

**What you should see:**
```
NAME                        STATUS
graph-updater-dev           Up (healthy)
graph-updater-kafka         Up (healthy)
graph-updater-neo4j         Up (healthy)
graph-updater-zookeeper     Up (healthy)
```

If you see "starting" instead of "healthy", wait a bit longer.

**Quick health check:**
```bash
curl http://localhost:8004/health
```

**What you should see:**
```json
{"status":"healthy","service":"graph-updater","timestamp":"2024-...","version":"0.4.0"}
```

### Step 4: Start the Dashboard

Open a **new terminal window** (keep the old one open), then:

```bash
cd /Users/dev/Documents/PROJECT_LOWLEVEL/DocWeave/services/graph-updater/dashboard

# First time only - install dependencies (takes 1-2 minutes)
npm install

# Start the dashboard
npm run dev
```

**What you should see:**
```
  ▲ Next.js 16.x.x
  - Local:        http://localhost:3000
  - Ready in 2.3s
```

### Step 5: Open the Dashboard

Open your web browser and go to: **http://localhost:3000**

**What you should see:** A dark-themed dashboard with:
- "Dashboard" header at the top
- Stats cards showing "Claims Processed", "Consumer Status", etc.
- A "Service Health" panel on the left
- A "Recent Conflicts" panel on the right

🎉 **Congratulations! The system is running!**

---

## Part 3: Your First Walkthrough (Learning to Drive)

Now let's actually USE the system. We'll create a small knowledge graph about a fictional company.

### Scenario: You're Building a Company Database

You have documents about tech companies and want to organize the facts.

### Tutorial 1: Creating Your First Entity

An **entity** is a "thing" - a person, company, place, concept, etc.

**Step 1:** Click **"Entities"** in the left sidebar

**What you see:** An empty entities page with a search bar and "Create Entity" button

**Step 2:** Click the **"Create Entity"** button (top right)

**Step 3:** Fill in the form:
- **Name:** `Acme Corporation`
- **Entity Type:** `ORGANIZATION`
- **Aliases:** `Acme Corp, Acme, ACME Inc`

**Step 4:** Click **"Create"**

**What happened:** You just created a node in the knowledge graph. Think of it as creating a folder for all facts about Acme Corporation.

**Step 5:** Create two more entities:

| Name | Type | Aliases |
|------|------|---------|
| `John Smith` | `PERSON` | `J. Smith, Johnny` |
| `San Francisco` | `LOCATION` | `SF, San Fran` |

Now you have 3 entities. Search for "Acme" in the search bar - you should see your company!

### Tutorial 2: Adding Your First Claim

A **claim** is a fact that connects entities or gives them properties.

**Step 1:** Click **"Claims"** in the left sidebar

**Step 2:** Click **"Add Claim"** button

**Step 3:** Fill in the form:
- **Subject Entity ID:** (copy the ID of Acme Corporation from the Entities page - it looks like `ent_xxxxx`)
- **Predicate:** `headquarters`
- **Value:** `San Francisco`
- **Source ID:** `annual_report_2024`
- **Confidence:** `0.95` (95% confident this is true)
- **Check for conflicts:** ✓ (keep checked)

**Step 4:** Click **"Add Claim"**

**What happened:** You just recorded the fact "Acme Corporation is headquartered in San Francisco" with a 95% confidence level, sourced from their annual report.

**Step 5:** Add more claims:

| Subject | Predicate | Value | Source | Confidence |
|---------|-----------|-------|--------|------------|
| Acme Corporation | employee_count | 15000 | annual_report_2024 | 0.95 |
| Acme Corporation | founded_year | 2010 | annual_report_2024 | 0.90 |
| Acme Corporation | ceo | John Smith | press_release_2024 | 0.85 |
| John Smith | job_title | CEO | linkedin_profile | 0.80 |

### Tutorial 3: Creating a Conflict (On Purpose!)

Let's see what happens when we have conflicting information.

**Step 1:** Go to Claims → Add Claim

**Step 2:** Add a conflicting claim:
- **Subject Entity ID:** (Acme Corporation's ID)
- **Predicate:** `employee_count`
- **Value:** `14500`
- **Source ID:** `news_article_2024`
- **Confidence:** `0.70`

**Step 3:** Click "Add Claim"

**What you should see:** A warning message saying "Claim added with 1 conflict(s) detected"

**What happened:** The system noticed that you already have a claim saying Acme has 15,000 employees, and now you're saying 14,500. It flagged this as a conflict.

### Tutorial 4: Viewing and Resolving Conflicts

**Step 1:** Click **"Conflicts"** in the left sidebar

**What you see:** A table showing your conflict:
- Entity: Acme Corporation
- Predicate: employee_count
- Conflicting Values: 15000 vs 14500
- Confidence scores for each

**Step 2:** Click the **gavel icon** (⚖️) on the conflict row

**What you see:** A resolution dialog showing:
- Both competing values
- Their confidence scores
- Resolution strategy options

**Step 3:** Choose a resolution:

**Option A - Select a winner manually:**
- Click on the "15000" card (the annual report one)
- Select strategy: "Manual Review"
- Add reason: "Annual report is more authoritative than news article"
- Click "Resolve Conflict"

**Option B - Let the system decide:**
- Select strategy: "Confidence Based"
- Click "Resolve Conflict"
- The system picks the 95% confidence claim over the 70% one

**What happened:** The losing claim is marked as "superseded" and won't be used for truth calculations.

### Tutorial 5: Viewing the Truth

Now let's see what the system "believes" about Acme Corporation.

**Step 1:** Click **"Truth Layer"** in the left sidebar

**Step 2:** Search for "Acme" and click on Acme Corporation

**What you see:** A table of truth values:

| Predicate | Value | Confidence | Sources |
|-----------|-------|------------|---------|
| headquarters | San Francisco | 95% | 1 |
| employee_count | 15000 | 95% | 1 |
| founded_year | 2010 | 90% | 1 |
| ceo | John Smith | 85% | 1 |

This is the system's "best knowledge" about Acme - conflicts resolved, sources tracked.

### Tutorial 6: Exploring the Graph Visually

**Step 1:** Click **"Graph Explorer"** in the left sidebar

**Step 2:** Search for "Acme" and click on it

**What you see:** A visual graph with:
- Acme Corporation in the center
- Lines (edges) connecting to related concepts
- You can drag nodes around
- Double-click a node to re-center on it

**Step 3:** Try changing the "Depth" to 3 to see more connections

---

## Part 4: Understanding What You're Seeing

### The Dashboard Home Page

```
┌─────────────────────────────────────────────────────────────────┐
│  Dashboard                                          [Refresh]   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────┐│
│  │ Claims       │ │ Consumer     │ │ Active       │ │ System  ││
│  │ Processed    │ │ Status       │ │ Conflicts    │ │ Status  ││
│  │    127       │ │   Active     │ │     3        │ │ Healthy ││
│  └──────────────┘ └──────────────┘ └──────────────┘ └─────────┘│
│                                                                 │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐  │
│  │ Service Health          │  │ Recent Conflicts            │  │
│  │ ✓ Neo4j Database    OK  │  │ • employee_count mismatch   │  │
│  │ ✓ Kafka Consumer    OK  │  │ • founded_year conflict     │  │
│  │ ✓ Graph Operations  OK  │  │ • ...                       │  │
│  │ ✓ Conflict Detector OK  │  │                             │  │
│  │ ✓ Truth Layer       OK  │  │           [View all →]      │  │
│  └─────────────────────────┘  └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**What each thing means:**

- **Claims Processed:** How many facts have been added to the system
- **Consumer Status:** Is the Kafka message processor running?
- **Active Conflicts:** Facts that contradict each other (need attention!)
- **System Status:** Overall health - green is good!

### The Conflict Resolution Strategies

When two claims conflict, you can resolve them using:

| Strategy | What It Does | When to Use |
|----------|--------------|-------------|
| **Timestamp Based** | Newer claim wins | When recent info is more accurate |
| **Confidence Based** | Higher confidence wins | When you trust the extraction quality |
| **Source Reliability** | Trusted source wins | When you know some sources are better |
| **Manual Review** | You pick the winner | When automated rules don't apply |

---

## Part 5: Common Tasks (Cheat Sheet)

### "I want to add information about a new company"

1. **Entities** → Create Entity → Fill in company name, type=ORGANIZATION
2. **Claims** → Add claims for each fact (headquarters, CEO, revenue, etc.)

### "I see a conflict - what do I do?"

1. **Conflicts** → Find the conflict
2. Look at both values and their confidence scores
3. Click the gavel icon
4. Either pick a winner manually OR choose an auto-resolution strategy

### "I want to see everything we know about X"

1. **Truth Layer** → Search for X
2. See all resolved truth values with confidence scores

### "I want to see how things are connected"

1. **Graph Explorer** → Search for an entity
2. Adjust depth to see more/fewer connections
3. Double-click nodes to explore

### "Something seems broken"

1. Go to **Admin** page
2. Check "Service Health Checks" - all should be green
3. If Kafka Consumer is red, click "Restart Consumers"
4. Check the dashboard home page for system status

---

## Part 6: Shutting Down (Parking the Car)

### Stop the Dashboard

In the terminal running the dashboard, press **Ctrl+C**

### Stop the Backend Services

```bash
cd /Users/dev/Documents/PROJECT_LOWLEVEL/DocWeave/services/graph-updater

# Stop services but keep your data
docker-compose down

# Stop services AND delete all data (fresh start)
docker-compose down -v
```

### Starting Again Tomorrow

```bash
# Start backend
cd /Users/dev/Documents/PROJECT_LOWLEVEL/DocWeave/services/graph-updater
docker-compose up -d

# Start dashboard (new terminal)
cd /Users/dev/Documents/PROJECT_LOWLEVEL/DocWeave/services/graph-updater/dashboard
npm run dev
```

---

## Part 7: Troubleshooting (When Things Go Wrong)

### "The dashboard shows errors / can't connect"

**Check 1:** Is the API running?
```bash
curl http://localhost:8004/health
```
If you get an error, the API isn't running. Run `docker-compose up -d`

**Check 2:** Is the .env.local file correct?
```bash
cat dashboard/.env.local
```
Should show: `NEXT_PUBLIC_API_URL=http://localhost:8004`

### "docker-compose up shows errors"

**Try this:**
```bash
# Stop everything
docker-compose down -v

# Remove old containers
docker system prune -f

# Start fresh
docker-compose up -d
```

### "Neo4j won't start"

Usually a port conflict. Check if something else is using port 7474:
```bash
lsof -i :7474
```

### "Everything is slow"

Make sure Docker has enough resources:
1. Open Docker Desktop
2. Go to Settings → Resources
3. Allocate at least 4GB memory, 2 CPUs

---

## Part 8: Glossary (New Vocabulary)

| Term | Meaning | Example |
|------|---------|---------|
| **Entity** | A thing (person, place, company, concept) | "Tesla", "Elon Musk", "California" |
| **Claim** | A fact connecting entities or giving properties | "Tesla was founded in 2003" |
| **Predicate** | The type of relationship or property | "founded_in", "has_ceo", "headquarters" |
| **Conflict** | Two claims that contradict each other | founded_in: 2003 vs 2004 |
| **Truth** | The resolved, best-known value after conflict resolution | "Tesla was founded in 2003" (95% confidence) |
| **Source** | Where a claim came from | "annual_report_2024", "wikipedia" |
| **Confidence** | How sure we are (0.0 to 1.0) | 0.95 = 95% confident |
| **Superseded** | A claim that lost to another in conflict resolution | The old value that was replaced |

---

## Quick Reference Card

```
┌────────────────────────────────────────────────────────────────┐
│                    QUICK REFERENCE                              │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  START EVERYTHING                                               │
│  $ cd services/graph-updater                                    │
│  $ docker-compose up -d                                         │
│  $ cd dashboard && npm run dev                                  │
│                                                                 │
│  URLS                                                           │
│  Dashboard ........... http://localhost:3000                    │
│  API Docs ............ http://localhost:8004/docs               │
│  Neo4j Browser ....... http://localhost:7474                    │
│                        (user: neo4j, pass: devpassword123)      │
│                                                                 │
│  STOP EVERYTHING                                                │
│  $ docker-compose down    (keeps data)                          │
│  $ docker-compose down -v (deletes data)                        │
│                                                                 │
│  HEALTH CHECK                                                   │
│  $ curl http://localhost:8004/health                            │
│  $ docker-compose ps                                            │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

---

**You're ready to go!** Start with the tutorials in Part 3, and refer back to this guide whenever you need help.
