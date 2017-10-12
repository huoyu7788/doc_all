# -*- coding:utf8 -*-
"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
import os

cur_abs_dir = os.path.dirname(os.path.abspath(__file__))
airflow_app_dir=os.path.dirname(cur_abs_dir)

thirdpay_home=os.path.join(airflow_app_dir,'thirdpay')

yesterday = datetime.combine(datetime.today() - timedelta(1),datetime.min.time())

default_args = {
    'owner': 'zhaoning',
    'depends_on_past': False,
    'start_date': yesterday,
    'email': ['zhaoning@lsh123.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('thirdpay', default_args=default_args,schedule_interval='0 12 * * *')

EXECUTION_DATE="{{ ds }}"


start=BashOperator(
    task_id='start',
    bash_command='echo start ',
    dag=dag
)

# ods 表生成 

ods_fin_third_pay_wxapp=BashOperator(
    task_id='ods_fin_third_pay_wxapp',
    bash_command='CheckTag -d {{ ds }} -l day  -b ods.ods_fin_third_pay_wxapp',
    dag=dag
)
ods_fin_third_pay_wxapp.set_upstream(start)

ods_fin_third_pay_wxgzh=BashOperator(
    task_id='ods_fin_third_pay_wxgzh',
    bash_command='CheckTag -d {{ ds }} -l day  -b ods.ods_fin_third_pay_wxgzh',
    dag=dag
)
ods_fin_third_pay_wxgzh.set_upstream(start)


ods_fin_third_pay_wxsm=BashOperator(
    task_id='ods_fin_third_pay_wxsm',
    bash_command='CheckTag -d {{ ds }} -l day  -b ods.ods_fin_third_pay_wxsm',
    dag=dag
)
ods_fin_third_pay_wxsm.set_upstream(start)

ods_fin_third_pay_alism=BashOperator(
    task_id='ods_fin_third_pay_alism',
    bash_command='CheckTag -d {{ ds }} -l day  -b ods.ods_fin_third_pay_alism',
    dag=dag
)
ods_fin_third_pay_alism.set_upstream(start)


ods_fin_third_pay_zfb=BashOperator(
    task_id='ods_fin_third_pay_zfb',
    bash_command='CheckTag -d {{ ds }} -l day  -b ods.ods_fin_third_pay_zfb',
    dag=dag
)
ods_fin_third_pay_zfb.set_upstream(start)

ods_fin_third_pay_lkl=BashOperator(
    task_id='ods_fin_third_pay_lkl',
    bash_command='CheckTag -d {{ ds }} -l day  -b ods.ods_fin_third_pay_lkl',
    dag=dag
)
ods_fin_third_pay_lkl.set_upstream(start)

# mds 数据处理依赖


mds_fin_third_pay_base_cmd = thirdpay_home+'/script/mds/mds_fin_third_pay_base.sh '+'{{ ds }}'
mds_fin_third_pay_cmd = thirdpay_home+'/script/mds/mds_fin_third_pay.sh '+'{{ ds }}'

    
mds_fin_third_pay_base = BashOperator(
    task_id='mds_fin_third_pay_base',
    bash_command=mds_fin_third_pay_base_cmd,
    dag=dag)

mds_fin_third_pay_base.set_upstream(ods_fin_third_pay_wxapp)
mds_fin_third_pay_base.set_upstream(ods_fin_third_pay_wxgzh)
mds_fin_third_pay_base.set_upstream(ods_fin_third_pay_alism)
mds_fin_third_pay_base.set_upstream(ods_fin_third_pay_wxsm)
mds_fin_third_pay_base.set_upstream(ods_fin_third_pay_lkl)
mds_fin_third_pay_base.set_upstream(ods_fin_third_pay_zfb)


mds_fin_third_pay = BashOperator(
    task_id='mds_fin_third_pay',
    bash_command=mds_fin_third_pay_cmd,
    dag=dag)

mds_fin_third_pay.set_upstream(mds_fin_third_pay_base)

#微信、支付宝生成凭证基础数据
mds_fin_third_pay_no_lkl_cmd = thirdpay_home+'/script/mds/mds_fin_third_pay_no_lkl.sh '+'{{ ds }}'
mds_fin_third_pay_lkl_cmd = thirdpay_home+'/script/mds/mds_fin_third_pay_lkl.sh '+'{{ ds }}'


mds_fin_third_pay_no_lkl = BashOperator(
    task_id='mds_fin_third_pay_no_lkl',
    bash_command=mds_fin_third_pay_no_lkl_cmd,
    dag=dag)

mds_fin_third_pay_no_lkl.set_upstream(mds_fin_third_pay)

mds_fin_third_pay_lkl = BashOperator(
    task_id='mds_fin_third_pay_lkl',
    bash_command=mds_fin_third_pay_lkl_cmd,
    dag=dag)

mds_fin_third_pay_lkl.set_upstream(mds_fin_third_pay)

#生成文件并写入
file_data_cmd = thirdpay_home+'/script/script/file_data.sh '+'{{ ds }}'

file_data = BashOperator(
    task_id='file_data',
    bash_command=file_data_cmd,
    dag=dag)

file_data.set_upstream(mds_fin_third_pay_no_lkl)
file_data.set_upstream(mds_fin_third_pay_lkl)


