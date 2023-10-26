import pandas as pd

def process_data(task_id_to_pull, processing_type, **kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids=task_id_to_pull)

    # 데이터 전처리 로직 (임시)
    if processing_type == 'uppercase':
        df['content'] = df['content'].str.upper()
    elif processing_type == 'lowercase':
        df['content'] = df['content'].str.lower()
    else:
        print(f"Unknown processing_type: {processing_type}")

    print(df)
    
    return df