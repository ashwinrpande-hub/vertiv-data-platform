# Vertiv DataMesh Marketplace UI

Independent frontend + backend for data product discovery.

## Stack
- Frontend: Vanilla HTML/JS (marketplace-ui/frontend/index.html)
- Backend: FastAPI + Mangum (AWS Lambda)
- AI Search: Snowflake Cortex EMBED_TEXT_768

## Local Dev
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000

## AWS Free Tier Deployment
- Frontend: S3 + CloudFront
- Backend: Lambda + API Gateway
- Cost: ~\/month on free tier

## API Endpoints
GET  /api/products          - List all data products
GET  /api/products/{id}     - Product detail
GET  /api/products/{id}/schema  - Output contract
GET  /api/products/{id}/trust   - DQ trust score
GET  /api/search?q=...      - AI semantic search
GET  /api/marketplace       - Full discovery catalog
POST /api/access/request    - Submit access request
GET  /docs                  - Interactive API docs
