FROM node:24-bookworm-slim AS web-assets

WORKDIR /web

COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY web/*.html web/tailwind.config.js web/tailwind.input.css ./
RUN npm run build:css

FROM python:3.14-slim

WORKDIR /app

# Use Tsinghua mirrors for apt and pip
RUN sed -i 's|deb.debian.org|mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list.d/debian.sources \
    && sed -i 's|security.debian.org|mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list.d/debian.sources

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

# Copy application code
COPY . .
COPY --from=web-assets /web/tailwind.css /app/web/tailwind.css

# Create necessary directories
RUN mkdir -p /app/web /app/certs

EXPOSE 38888

CMD ["python", "start.py"]
