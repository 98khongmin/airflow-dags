import xml.etree.ElementTree as ET
from datetime import datetime
import requests
import urllib.parse

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


MY_NAME = "kimhongmin"  # 본인 이름 입력
S3_BUCKET_NAME = f"realestate-{MY_NAME}"
AWS_CONN_ID = "aws_default"

# 6개 수집 대상 시군구 코드
LAWD_CD_LIST = [
    "11680",
    "11650",
    "11710",
    "11440",
    "11170",
    "11200",
]


def collect_and_upload_to_s3(lawd_cd: str, **context):
    # 첫 줄 print 강제 출력
    print(f"collector={MY_NAME}, time={datetime.now()}, lawd={lawd_cd}")

    # execution_date 기준 처리 월 (yyyymm) 추출
    logical_date = context["logical_date"]
    deal_ymd = logical_date.strftime("%Y%m")

    # 국토교통부 아파트 매매 실거래가 API 호출
    api_url = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
    )
    raw_service_key = Variable.get("data_go_kr_service_key")

    # unquote를 통해 이중 인코딩 방지
    service_key = urllib.parse.unquote(raw_service_key)

    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": "1",
        "numOfRows": "1000",
    }

    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        xml_content = response.text

        # XML 파싱 및 검증
        root = ET.fromstring(xml_content)
        result_code = root.findtext(".//resultCode")

        if result_code not in ["00", "000"]:
            print(
                f"[{lawd_cd}] API 응답 비정상: {result_code} - {root.findtext('.//resultMsg')}"
            )
            return {"status": "FAIL", "count": 0}

        total_count_elem = root.findtext(".//totalCount")
        total_count = int(total_count_elem) if total_count_elem else 0

        # 거래 건수
        items = root.findall(".//item")
        actual_count = len(items)

        if actual_count == 0 and total_count == 0:
            print(f"[{lawd_cd}] 거래 건수 0건")
            return {"status": "NO_DATA", "count": 0}

        # S3 업로드: bronze/{yyyymm}/{LAWD_CD}.xml
        s3_key = f"bronze/{deal_ymd}/{lawd_cd}.xml"
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_string(
            string_data=xml_content,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
            encoding="utf-8",
        )
        print(
            f"[{lawd_cd}] S3 업로드 성공: s3://{S3_BUCKET_NAME}/{s3_key} (총 {actual_count}건)"
        )
        return {"status": "SUCCESS", "count": actual_count}

    except ET.ParseError as pe:
        print(f"[{lawd_cd}] XML 파싱 실패: {pe}")
        return {"status": "FAIL", "count": 0}
    except Exception as e:
        print(f"[{lawd_cd}] 수집 중 에러 발생: {e}")
        return {"status": "FAIL", "count": 0}


def check_branch_condition(**context):
    ti = context["task_instance"]
    success_count = 0

    for lawd_cd in LAWD_CD_LIST:
        task_id = f"collect_group.collect_{lawd_cd}"
        res = ti.xcom_pull(task_ids=task_id)

        if res and res.get("status") == "SUCCESS":
            success_count += 1

    print(f"수집 성공 지역 개수: {success_count} / {len(LAWD_CD_LIST)}")

    if success_count > 0:
        return "summary_done"
    return "skip_upload"


with DAG(
    dag_id="bronze_realestate_collect",
    schedule="@monthly",
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2025, 1, 1),  # 2024년 12월 1회차만 실행되도록 제한
    catchup=True,
    tags=["bronze", "realestate"],
) as dag:

    # TaskGroup을 이용한 6개 시군구 병렬 수집
    with TaskGroup(group_id="collect_group") as collect_group:
        for lawd in LAWD_CD_LIST:
            PythonOperator(
                task_id=f"collect_{lawd}",
                python_callable=collect_and_upload_to_s3,
                op_kwargs={"lawd_cd": lawd},
            )

    # 파싱 실패 여부에 따른 분기 태스크
    branch_after_collect = BranchPythonOperator(
        task_id="branch_after_collect", python_callable=check_branch_condition
    )

    # 분기 대상
    summary_done = EmptyOperator(task_id="summary_done")
    skip_upload = EmptyOperator(task_id="skip_upload")

    collect_group >> branch_after_collect >> [summary_done, skip_upload]