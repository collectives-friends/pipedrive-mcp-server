# Multi-stage build for optimized image size
FROM node:20-alpine AS builder

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY src ./src

# Build the application
RUN npm run build

# Production stage
FROM node:20-alpine AS production

# Install wget for healthcheck
RUN apk add --no-cache wget

# Set working directory
WORKDIR /app

# Copy package files and built application from builder stage
COPY --from=builder /app/package.json /app/package-lock.json ./
COPY --from=builder /app/build ./build

# Install production dependencies only
RUN npm ci --omit=dev

# Set NODE_ENV to production
ENV NODE_ENV=production

# Expose the default MCP port (can be overridden via environment variable)
EXPOSE 3000

# Run as non-root user for security
USER node

# Start the application
CMD ["node", "build/index.js"]
