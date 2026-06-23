# pg2ch 런타임 이미지 = Airflow 3.2.2 + clickhouse-driver + pg2ch 엔진 패키지.
FROM apache/airflow:3.2.2

# ── 사설 패키지 저장소(미러) 위치 (빌드 타임) ─────────────────────────
# 전부 선택적. compose 의 build.args 또는 `docker build --build-arg` 로 주입한다.
# 비어 있으면 평소처럼 공용 PyPI 를 탄다.
#   - PIP_* : 사내 PyPI 미러 (이 이미지는 pip 로 설치하므로 이게 실제로 쓰임)
#   - UV_INDEX_URL / NPM_CONFIG_REGISTRY : uv / npm 을 빌드에서 쓸 경우용 plumbing
ARG PIP_INDEX_URL=
ARG PIP_EXTRA_INDEX_URL=
ARG PIP_TRUSTED_HOST=
ARG UV_INDEX_URL=
ARG NPM_CONFIG_REGISTRY=

# ARG → ENV 승격. pip / uv / npm 모두 아래 환경변수를 표준으로 읽으므로
# 추가 설정 없이 사설 미러를 탄다. 값이 비면 무시된다.
ENV PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} \
    UV_INDEX_URL=${UV_INDEX_URL} \
    NPM_CONFIG_REGISTRY=${NPM_CONFIG_REGISTRY}

# 추가 런타임 의존성
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# pg2ch 엔진 패키지 설치 ('pg2ch' CLI entrypoint 포함).
# dags/ 와 config/ 는 이미지에 굽지 않고 compose 에서 bind-mount 한다
# (DAG/설정 변경 시 재빌드 불필요). 엔진 코드 변경 시에만 이미지 재빌드.
COPY pyproject.toml /tmp/pg2ch/pyproject.toml
COPY pg2ch /tmp/pg2ch/pg2ch
RUN pip install --no-cache-dir /tmp/pg2ch
