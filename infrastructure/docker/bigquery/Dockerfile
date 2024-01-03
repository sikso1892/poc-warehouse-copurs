# 사용할 기본 이미지
FROM python:3.8-slim

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 도구 설치 (Google Cloud SDK, etc.)
RUN apt-get update && apt-get install -y curl gnupg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN apt-get update && apt-get install -y google-cloud-sdk
RUN apt-get install -y gcc python3-dev

# Python 라이브러리 설치
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Jupyter 노트북 실행 (토큰 및 비밀번호 없음 - 개발/테스트 환경에서만 사용)
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]