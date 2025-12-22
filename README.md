# Doc Router Temporal

A Temporal workflow system for listing documents from docrouter via FastAPI.

## Overview

This project provides a Temporal workflow that connects to the docrouter FastAPI service to list documents in a workspace (organization). It demonstrates how to integrate Temporal workflows with external FastAPI services.

## Features

- Temporal workflow for listing documents from docrouter
- Async activity that calls FastAPI endpoints
- Configurable connection to Temporal server
- Easy-to-use client script to trigger workflows

## Prerequisites

- Python 3.9+
- Temporal server running at `10.83.8.98` (or configured via environment variables)
- Access to docrouter FastAPI service
- API token for docrouter authentication

## Installation

1. Clone the repository:
```bash
cd doc-router-temporal
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
cp env.example .env
# Edit .env with your configuration
```

Required environment variables:
- `DOCROUTER_BASE_URL`: Base URL of the docrouter FastAPI service (e.g., `http://localhost:8000`)
- `DOCROUTER_API_TOKEN`: API token for authentication
- `TEMPORAL_HOST`: Temporal server host (default: `10.83.8.98`)
- `TEMPORAL_PORT`: Temporal server port (default: `7233`)
- `TEMPORAL_NAMESPACE`: Temporal namespace (default: `default`)
- `ORGANIZATION_ID`: The organization/workspace ID to list documents from

## Usage

### Starting the Worker

First, start the Temporal worker to process workflows:

```bash
python worker.py
```

The worker will:
- Connect to Temporal at the configured host
- Listen on the `doc-router-task-queue` task queue
- Execute workflows and activities

### Running a Workflow

In a separate terminal, run the client to start a workflow:

```bash
python client.py
```

The client will:
- Connect to Temporal
- Start a workflow to list documents for the configured organization
- Wait for the workflow to complete
- Display the results

### Workflow Details

The workflow (`ListDocumentsWorkflow`) performs the following:
1. Receives an organization ID and pagination parameters
2. Calls an activity to list documents from docrouter FastAPI
3. Returns the list of documents with metadata

The activity (`list_documents_activity`) makes an HTTP GET request to:
```
GET /v0/orgs/{organization_id}/documents?skip={skip}&limit={limit}
```

## Project Structure

```
doc-router-temporal/
├── workflows/
│   ├── __init__.py
│   └── list_documents.py      # Temporal workflow definition
├── activities/
│   ├── __init__.py
│   └── list_documents.py      # Activity that calls FastAPI
├── worker.py                   # Temporal worker
├── client.py                   # Client to start workflows
├── requirements.txt            # Python dependencies
├── env.example                 # Environment variable template
└── README.md                   # This file
```

## Configuration

### Temporal Server

The default configuration connects to Temporal at `10.83.8.98:7233` with the `default` namespace. You can override these via environment variables.

### DocRouter API

The activity connects to the docrouter FastAPI service. Ensure:
- The service is accessible at the configured `DOCROUTER_BASE_URL`
- You have a valid `DOCROUTER_API_TOKEN`
- The organization ID exists and you have access to it

## Development

### Adding New Workflows

1. Create a new workflow class in `workflows/`
2. Define activities in `activities/`
3. Register them in `worker.py`
4. Create a client script if needed

### Testing

To test the workflow:
1. Ensure docrouter FastAPI is running
2. Start the worker: `python worker.py`
3. Run the client: `python client.py`

## License

[Add your license here]