import pandas as pd
import os

def export_to_parquet(task_id_to_pull, project_nm, **kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids=task_id_to_pull)

    # 현재 디렉토리에 file_export 경로 생성
    output_path = os.path.join(os.getcwd(), 'file_export', project_nm, project_nm)
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # parquet 파일로 저장
    file_path = os.path.join(output_path, project_nm, f"{project_nm}.parquet")
    df.to_parquet(file_path)

    print(f"Data saved to {file_path}")
