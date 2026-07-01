# muse-web — the MUSE console (Vite static build) served by nginx, which also
# reverse-proxies /v1 (incl. the SSE stream) to muse-server. Build context is the
# repo root (`build-context: "."`).
FROM node:22-bookworm-slim AS builder
WORKDIR /web
# No lockfile is committed (gitignored), so install then build.
COPY muse/web/package.json ./
RUN npm install
COPY muse/web/ ./
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine
# The backend URL is config-driven: nginx's entrypoint renders templates in
# /etc/nginx/templates via envsubst at startup. The default targets the
# in-cluster Service and is meant to be overridden per environment (Helm / deploy
# env), not relied on as a baked-in constant.
ENV MUSE_BACKEND=http://muse-server:8080
COPY dockerfiles/muse-web.nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=builder /web/dist /usr/share/nginx/html
EXPOSE 8080
