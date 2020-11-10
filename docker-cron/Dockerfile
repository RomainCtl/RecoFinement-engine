FROM alpine:latest

RUN apk update && apk add curl

# Copy cron file to the cron.d directory
COPY recommend /etc/cron.d/recommend

# Give execution rights on the cron jobs
RUN chmod 644 /etc/cron.d/recommend

# Apply cron job
RUN crontab /etc/cron.d/recommend

CMD crond -f
