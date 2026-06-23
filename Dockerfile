# pg2ch 런타임 이미지 = Airflow 3.2.2 + clickhouse-driver + pg2ch 엔진 패키지.
FROM apache/airflow:3.2.2

# 추가 런타임 의존성
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# pg2ch 엔진 패키지 설치 ('pg2ch' CLI entrypoint 포함).
# dags/ 와 config/ 는 이미지에 굽지 않고 compose 에서 bind-mount 한다
# (DAG/설정 변경 시 재빌드 불필요). 엔진 코드 변경 시에만 이미지 재빌드.
COPY pyproject.toml /tmp/pg2ch/pyproject.toml
COPY pg2ch /tmp/pg2ch/pg2ch
RUN pip install --no-cache-dir /tmp/pg2ch
