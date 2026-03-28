# Agent Thala
<img width="1024" height="1024" alt="image-Photoroom" src="Logo.png" />

![made-with-python](https://img.shields.io/badge/Made%20with-Python3-brightgreen)
![Flask](https://img.shields.io/badge/Flask-000?logo=flask&logoColor=fff) ![Python 3.12+](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=fff)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?logo=apachekafka&logoColor=fff)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-0377CC?logo=elasticsearch&logoColor=fff)
![AWS Bedrock](https://img.shields.io/badge/AWS%20Bedrock-FF9900?logo=aws&logoColor=fff) ![Hugging Face](https://img.shields.io/badge/Hugging%20Face-FFD21E?logo=huggingface&logoColor=000)
![AWS S3](https://img.shields.io/badge/Amazon%20S3-FF9900?logo=amazons3&logoColor=fff) ![AWS Textract](https://img.shields.io/badge/AWS%20Textract-FF9900?logo=aws&logoColor=fff)
![Nginx](https://img.shields.io/badge/nginx-009639?logo=nginx&logoColor=fff)
![Slack](https://img.shields.io/badge/Slack-4A154B?logo=slack&logoColor=fff) ![Jira](https://img.shields.io/badge/Jira-0052CC?logo=jira&logoColor=fff)
![boto3](https://img.shields.io/badge/boto3-FF9900?logo=aws&logoColor=fff)
![beautifulsoup4](https://img.shields.io/badge/beautifulsoup4-000?logo=python&logoColor=fff)

<!-- LOGO -->
<!-- LOGO -->

<!-- <img width="455" height="549" alt="Gemini_Generated_Image_rahzs0rahzs0rahz-removebg-preview" src="https://github.com/user-attachments/assets/9ed75804-094a-4626-95e1-6c0dd28eb34a" /> -->

<!-- <p align="center">
  <a href="https://youtu.be/Uw5dVqHBSAs?si=99UPnVoGxeMgcwxF&t=1970" target="_blank">
    <img width="455" height="549" alt="SuperHack 2025 Winner Badge" src="https://github.com/user-attachments/assets/9ed75804-094a-4626-95e1-6c0dd28eb34a" />
  </a>
  <a href="https://gitcgr.com/SaiNivedh26/team-thala" target="_blank" rel="noopener noreferrer">
    <img src="https://gitcgr.com/badge/SaiNivedh26/team-thala.svg" alt="gitcgr" />
  </a>
</p> -->




<br>
<br>

<br />
<h1 align="center">
  <font size="7">THALA</font>
</h1>
  <p align="center">
    Intelligent incident management system for automated detection, classification, and resolution tracking.
    <br />
    </p>
</p>

<p align="center">
  <a href="#about-the-project">About The Project</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#key-features">Key Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#usage">Usage</a> •
  <a href="#api-endpoints">API Endpoints</a>
</p>

<br>

<div align="center">
  <h3>For Any Support</h3>
  <a href="https://discord.gg/M6m9Pn6spK">
    <img src="https://img.shields.io/badge/Join%20Us%20On-Discord-5865F2?logo=discord&logoColor=white" alt="Join us on Discord" height="28">
  </a>
</div>

<br>

<div align="center">
  <h3>Zoho Workspace Support</h3>
  <a href="https://github.com/SaiNivedh26/workspace-vita">
    <img src="https://img.shields.io/badge/Supported%20Platform-Zoho%20Workspace-E42527?logo=zoho&logoColor=white" alt="Zoho Workspace" height="28">
  </a>
</div>



<br>
<br>

<!-- <h2>Want to try hands on ?</h2> -->

<img width="666" height="375" alt="Copy_of_mohan-review__5___1_-removebg-preview" src="https://github.com/user-attachments/assets/dc2e9235-6f10-480f-9764-ca6884571c0c" />

[![Agnent Thala Prototype Launch](https://github.com/user-attachments/assets/e449d585-6156-4b3b-8867-81849678defd)](https://try-thala-psi.vercel.app/)



<br>
<br>

<!-- <h2>Ready to check the end to end demo ?</h2> -->
<img width="666" height="375" alt="Copy_of_mohan-review__6___1_-removebg-preview" src="https://github.com/user-attachments/assets/cec1f8d7-3652-427d-ac0a-294d3e1f293d" />

<br>

[![Agnent Thala Prototype Launch](https://github.com/user-attachments/assets/c0d97319-7487-4c47-b27e-91f7b7029eef)](https://www.youtube.com/watch?v=pK1OFuGYdEE)

<br>
<br>
<img width="693" height="360" alt="Copy_of_mohan-review__4___1_-removebg-preview" src="https://github.com/user-attachments/assets/748d29fe-29ad-4554-bfe9-831e1956aa4b" />

[![mohan-review (1)](https://github.com/user-attachments/assets/74ca985c-9b7f-4c74-9822-ca0057aabb45)](https://www.linkedin.com/in/mohan~v/)
[![santosh](https://github.com/user-attachments/assets/d410d85d-1ca3-466d-a729-35d9eac3aba9)](https://www.linkedin.com/in/santhosh-sachin/)



<br>
<br>

--------------------------------------------------------------------------------

## About The Project

Thala is an intelligent incident management system that automatically:
- Detects incidents from Slack messages, Jira tickets, and emails
- Classifies & predicts severity, category, and likelihood using agent
- Tracks resolutions and links them to original incidents
- Searches similar past incidents using semantic similarity
- Extracts text from image attachments using AWS Textract

## Architecture
<img width="1912" height="1076" alt="architecture-thala" src="u-arch.jpg" />

## Data Flow

1. **Ingestion**: Slack/Jira/Email → Connectors → Kafka
2. **Classification**: Llama 3.3 70b LLM classifies messages (incident, resolution, discussion, unrelated)
3. **Prediction**: AWS Bedrock (llama-3.3-70b) agent predicts category & severity
4. **Attachment Processing**: Images → S3 → Textract → Extracted text → Context
5. **Storage**: Flask API → Elasticsearch (with embeddings for semantic search)
6. **Resolution Tracking**: Links resolution messages to original incidents
7. **UI**: Slack bot commands (/thala latest_issue, /thala search)

## Key Features

![clip7](https://github.com/user-attachments/assets/7aadff99-fc76-40ae-a629-e375f6ea6dfa)
![clipp6](https://github.com/user-attachments/assets/d544533f-2d71-4035-83f8-859038369e0d)
![clip4](https://github.com/user-attachments/assets/940ed5ef-5323-4604-bc83-c8cbcc14ef10)
![clip3](https://github.com/user-attachments/assets/2b53d1a8-a589-41d6-b235-e2b139874a1e)
![clipp2](https://github.com/user-attachments/assets/94c2489b-4c73-432d-8b4a-63298c82c171)
![clip1](https://github.com/user-attachments/assets/890c69c5-8cf1-4727-aad1-c45382df2dd1)





### 1. Intelligent Classification
- Uses LLM from AWS Bedrock (llama-3.3-70b) to classify messages semantically
- No keyword matching - pure agent understanding
- Types: incident_report, resolution, discussion, unrelated

### 2. Smart Resolution Linking
- Links vague resolutions ("auth issue fixed") to correct incidents
- Uses semantic similarity (embeddings) + conversational context
- Automatically marks incidents as "Resolved" in Elasticsearch

### 3. Attachment Processing (AWS Integration)
- Downloads images from Slack/Jira attachments
- Uploads to S3 bucket (thala-images)
- Extracts text using AWS Textract
- Adds extracted text to message context for classification

### 4. Incident Prediction
- **Category**: Database, API, Frontend, Infrastructure, Authentication, etc.
- **Severity**: Critical, High, Medium, Low
- **Likelihood**: Likely, Unlikely (for new queries)
- Uses Llama model with few-shot learning

### 5. Semantic Search
- Finds similar past incidents using vector embeddings
- Prioritizes resolved incidents with complete resolution info
- Returns similarity scores and resolution details

### 6. Slack Bot Commands
- `/thala latest_issue [page]` - View ongoing incidents (paginated)
- `/thala search <query>` - Search similar resolved incidents
- `/thala predict <description>` - Predict category/severity
- `/thala` - Show help

## Quick Start

### Prerequisites
- Python 3.12+
- Elasticsearch 9.1.5+ (running)
- Kafka (KRaft mode, optional for real-time)
- AWS Account (for S3 + Textract)

### Installation

Install dependencies:
```bash
pip install -r requirements.txt
pip install -r team-thala/src/ui_requirements.txt
```

### Environment Setup

Create `.env` file in the root directory:
```ini

GEMINI_API_KEY=
FLASK_API_URL=http://localhost:5000

# Elasticsearch Configuration (if remote, change localhost to your ES host)
ELASTICSEARCH_HOST=https://localhost:9200
SLACK_APP_TOKEN=

JIRA_URL=https://kphotos1803.atlassian.net
JIRA_EMAIL=
JIRA_API_TOKEN= 

SLACK_BOT_TOKEN=
SLACK_CHANNEL_ID=

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_SLACK=thala-slack-events
KAFKA_TOPIC_JIRA=thala-jira-events

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/thala_ingestion.log



# Elasticsearch Configuration (if remote, change localhost to your ES host)


AWS_LAMBDA_URL=



# Kafka Configuration
KAFKA_TOPIC_SLACK=thala-slack-events
KAFKA_TOPIC_JIRA=thala-jira-events


SEARCH_BACKEND=opensearch_serverless 
AWS_REGION=us-east-2


AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
AWS_SESSION_TOKEN="" 

FunctionUrl= ""
FunctionArn= ""


AWS_BEARER_TOKEN_BEDROCK="" 

OPENSEARCH_HOST = ""



KAFKA_BOOTSTRAP_SERVERS=""


REDIS_FALLBACK_ENABLED=true
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_LIST_PREFIX=thala:queue:

AWS_REGION=us-east-2
BEDROCK_LLAMA_MODEL_ID=meta.llama3-3-70b-instruct-v1:0

```

### Slack App Setup

1. Create Slack app at https://api.slack.com/apps
2. Add Bot Token Scopes:
   - `channels:history`, `channels:read`
   - `chat:write`, `commands`
   - `app_mentions:read`, `im:history`
   - `files:read` (REQUIRED for attachments)
3. Install app to workspace
4. Copy Bot Token (xoxb-...) to `.env`

**See**: `team-thala/SLACK_FILES_READ_SETUP.md` for detailed setup instructions.

## Usage

### Start All Services (Integrated)
```bash
python integrated_main.py
```

### Start Services Separately
```bash
# Terminal 1: Flask API
python new.py

# Terminal 2: Kafka Consumer
python team-thala/src/kafka_consumer_to_flask.py

# Terminal 3: Slack Connector
python team-thala/src/slack_connector_enhanced.py

# Terminal 4: Slack Bot UI
python team-thala/src/slack_bot_ui.py
```

### Slack Commands

```sh
/thala                          # Show help and available commands
/thala latest_issue [page]      # View ongoing incidents (paginated, 10 per page)
/thala search <query>           # Search similar resolved incidents
```

## How It Works

### Incident Creation Flow
```
Slack: "API server is down"
  → LLM from AWS Bedrock (llama-3.3-70b) classifies as "incident_report"
  → It predicts: Category=API, Severity=High
  → Sent to Kafka → Flask → Elasticsearch
  → Tracked in Incident Tracker
  → Available in Slack: /thala latest_issue
```

### Resolution Flow
```
Slack: "API issue has been fixed"
  → LLM from AWS Bedrock (llama-3.3-70b) classifies as "resolution"
  → Semantic search finds matching open incident
  → Updates status to "Resolved" in Elasticsearch
  → Logs resolution text, resolved_by, resolved_at
  → Removed from ongoing incidents list
```

### Attachment Flow
```
Slack: [Image attachment] "Check this error"
  → Download image from Slack (files_info API)
  → Upload to S3 bucket
  → Extract text using Textract
  → Add extracted text to message context
  → Classify with full context (image + text)
  → Create incident if classified as incident_report
```

### Search Flow
```
Slack: /thala search "database timeout"
  → Flask API performs semantic search in Elasticsearch
  → Returns similar resolved incidents
  → Prioritizes incidents with complete resolution info
  → Displays in Slack with rich formatting
```

## Key Components

### slack_connector_enhanced.py
- Monitors Slack channels for messages
- Classifies messages using LLM from AWS Bedrock (llama-3.3-70b)
- Processes attachments (S3 + Textract)
- Detects resolutions and links to incidents
- Prevents resolution messages from creating new incidents
- Handles vague messages intelligently

### slack_bot_ui.py
- Slack bot with slash commands
- Paginated incident listing
- Semantic search interface
- Rich UI with Slack Block Kit

### gemini_predictor.py (uses Llama model from AWS Bedrock now)
- Predicts category & severity
- Uses few-shot learning with training examples
- Caches predictions (24h TTL)

### aws_attachment_processor.py
- Downloads attachments from Slack/Jira
- Uploads to S3 bucket
- Extracts text using Textract
- Handles image format conversion (PNG → JPEG)

### new.py (Flask API)
- `/index` - Store incidents in Elasticsearch
- `/search` - Semantic similarity search
- `/predict_incident` - Predict likelihood
- `/update_status` - Mark incidents as resolved
- `/lookup_incident` - Find incident by ID

## API Endpoints

### POST /index
Store new incident in Elasticsearch
```json
{
  "texts": ["API server is down"],
  "timestamp": "2025-11-01T10:00:00",
  "status": "Open",
  "source": "slack",
  "category": "API",
  "severity": "High"
}
```

### POST /search
Semantic similarity search
```json
{
  "query": "database connection timeout",
  "top_k": 10
}
```

### POST /update_status
Mark incident as resolved
```json
{
  "issue_id": "slack_1234567890",
  "status": "Resolved",
  "resolution_text": "Fixed connection pool",
  "resolved_by": "U08L203J5TK",
  "resolved_at": "2025-11-01T10:15:00"
}
```

### POST /lookup_incident
Find incident by ID
```json
{
  "issue_id": "slack_1234567890"
}
```

## Important Notes

### Token Types
- **Bot Token** (xoxb-...): Required for Web API calls (files_info, channels, etc.)
- **App Token** (xapp-...): Only for Socket Mode (not used currently)
- **Use Bot Token** in SLACK_BOT_TOKEN environment variable


### Attachment Requirements
- Slack app must have `files:read` scope
- AWS credentials must be configured
- S3 bucket must exist (thala-images)
- Textract must be enabled in AWS region

### Resolution Detection
- No keyword matching - pure semantic understanding
- Links resolutions even if ID not mentioned explicitly
- Uses conversational context (recent incidents)
- Fallback to most recent open incident if no match


------

![Material wave loading](https://github.com/user-attachments/assets/a08255eb-9647-471d-9881-61871332249f)

## Developers

[Sai Nivedh](https://www.github.com/SaiNivedh26)

[Kishore Balaji](https://www.github.com/Kishore-1803)

<br>

### If you like this, then
[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/sai_nivedh_26)

### Still have some doubts ?
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/SaiNivedh26/team-thala)
