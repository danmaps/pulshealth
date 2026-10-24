# syntax=docker/dockerfile:1
# Multi-stage build producing a lean Next.js standalone server.
# Runs as a service in the Puls stack (server/docker-compose.yml), reaching
# Postgres over the internal docker network as the read-only `grafana` role —
# or, in accounts mode, as the per-user `web_app` role.

FROM node:26.12.1-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

# Explicit opt-in verification image. Production still ends at the runner stage.
# Uses pinned test dependencies and the same source as the deployed revision.
FROM deps AS read-check
COPY . .
ENV NODE_ENV=production
CMD ["npm", "exec", "--no", "--", "vitest", "run", "lib/readPerformance.test.ts", "--reporter=json", "--outputFile=/evidence/tests.json"]

FROM node:26.12.1-alpine AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

FROM node:26.12.1-alpine AS runner
WORKDIR /app
ARG BUILD_COMMIT=unknown
LABEL org.opencontainers.image.revision=$BUILD_COMMIT \
      org.opencontainers.image.source="https://github.com/PulsHealth/pulshealth"
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
RUN addgroup -S nodejs && adduser -S nextjs -G nodejs
RUN mkdir -p /deletion-ledger && chown nextjs:nodejs /deletion-ledger
# Standalone bundle + static assets (no public/ dir in this project).
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static
# The accounts-mode operator commands (`make web-invite`, `make web-demo`);
# they import pg, which the standalone bundle already carries for the server.
COPY --from=builder --chown=nextjs:nodejs /app/scripts/invite.mjs /app/scripts/demo.mjs ./scripts/
USER nextjs
EXPOSE 3000
CMD ["node", "server.js"]
