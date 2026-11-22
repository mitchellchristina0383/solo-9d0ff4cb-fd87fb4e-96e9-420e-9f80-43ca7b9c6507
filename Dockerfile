FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY requirements.txt /srv/requirements.txt
RUN pip install --no-cache-dir -r /srv/requirements.txt

COPY app /srv/app
COPY echo /srv/echo

EXPOSE 8000

# 默认跑主服务；回显线路在 compose 里用 command 覆盖为 python -m echo.server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
