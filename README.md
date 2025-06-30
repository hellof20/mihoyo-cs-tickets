# Mihoyo CS Tickets System

A full-stack application for managing and analyzing customer service tickets for Mihoyo. The system provides automated ticket clustering, issue summarization, and FAQ generation capabilities.

## Features

- **Ticket Clustering**: Automatically groups similar customer service tickets using advanced clustering algorithms
- **Issue Summarization**: Generates concise summaries of ticket clusters to identify common patterns
- **FAQ Generation**: Automatically creates FAQ entries based on common issues
- **Interactive UI**: React-based frontend for easy ticket management and analysis
- **Real-time Processing**: Handles ticket processing through Google Cloud PubSub
- **Data Analytics**: Leverages BigQuery for efficient data analysis

## Project Structure

```
.
├── mihoyo-cs-tickets-ui/    # React TypeScript frontend
├── sql/                     # SQL scripts for data processing
├── bq_handler.py           # BigQuery integration handler
├── cluster_issue.py        # Ticket clustering logic
├── config.toml             # Configuration file
├── main.py                 # Main application entry point
├── pubsub_handler.py       # PubSub message handling
└── summary_issue.py        # Issue summarization logic
```

## Prerequisites

- Python 3.x
- Node.js and npm
- Google Cloud Platform account with BigQuery and PubSub enabled

## Setup

1. **Backend Setup**
   ```bash
   # Install Python dependencies
   pip install -r requirements.txt

   # Configure the application
   # Edit config.toml with your settings
   ```

2. **Frontend Setup**
   ```bash
   cd mihoyo-cs-tickets-ui
   npm install
   ```

## Running the Application

1. **Start the Backend**
   ```bash
   uvicorn main:app --reload --port 8000
   ```

2. **Start the Frontend**
   ```bash
   cd mihoyo-cs-tickets-ui
   npm start
   ```

The application will be available at `http://localhost:3000`

## Development

### Backend Development

The backend is built with Python and consists of several key components:
- `main.py`: Application entry point and API endpoints
- `bq_handler.py`: Handles BigQuery operations
- `cluster_issue.py`: Implements ticket clustering logic
- `pubsub_handler.py`: Manages PubSub message processing
- `summary_issue.py`: Handles ticket summarization

### Frontend Development

The frontend is built with React and TypeScript, located in the `mihoyo-cs-tickets-ui` directory:
- `/src/components`: React components
- `/src/services`: API service integrations
- `/src/types`: TypeScript type definitions

### Database

SQL scripts in the `sql/` directory handle:
1. View creation
2. Issue summarization
3. Embedding generation
4. FAQ generation

## Configuration

Edit `config.toml` to configure:
- Database connections
- GCP settings
- Application parameters
- API endpoints

## Demo
![image](https://github.com/user-attachments/assets/0489a18c-54b1-405b-8e5b-8da1dbb5da5a)

![image](https://github.com/user-attachments/assets/6c67b0f0-c6f8-4e6e-93d5-53f6453aed35)

![image](https://github.com/user-attachments/assets/74630298-c9a0-4b00-85a2-dc3a4ca9e5e8)

![image](https://github.com/user-attachments/assets/c50b2713-c337-4906-bc19-bf909650e856)

## License

[Add your license information here]
