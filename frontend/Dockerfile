# Dockerfile for frontend

FROM node:18

WORKDIR /app

# Copy dependency files first (better Docker layer caching)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy application source
COPY . .

# Create production build
RUN npm run build

# Install a lightweight static file server
RUN npm install -g serve

# Document the port used by the container
EXPOSE 3006

# Serve the production build on port 3006
CMD ["serve", "-s", "build", "-l", "3006"]