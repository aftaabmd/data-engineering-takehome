import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.stream_data.Producer import generate_stream

PATH_STREAM_Customer = "/sample_data/Customer.txt"
PATH_STREAM_Customer_Extended = "/sample_data/Customer_Extended.txt"
PATH_STREAM_Product = "/sample_data/Product.txt"
PATH_STREAM_Sales= "/sample_data/Sales.txt"
PATH_STREAM_Refund = "/sample_data/Refund.txt"

Topic1 = 'Customer'
Topic2 = 'Customer_Extended'
Topic3 = 'Product'
Topic4 = 'Sales'
Toopic5 = 'Refund'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),      # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='data_stream_DAG',
    default_args=args,
    schedule_interval= '@once',             # set interval
    catchup=False,                          # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
)


task1 = PythonOperator(
    task_id='generate_stream1',
    python_callable=generate_stream,              # function to be executed
    op_kwargs={'path_stream': PATH_STREAM_Customer,        # input arguments
               'Topic': Topic1},
    dag=dag,
)

task2 = PythonOperator(
    task_id='generate_stream2',
    python_callable=generate_stream,
    op_kwargs={'path_stream': PATH_STREAM_Customer_Extended,
               'Topic': Topic2},
    dag=dag,
)

task3 = PythonOperator(
    task_id='generate_stream3',
    python_callable=generate_stream,
    op_kwargs={'path_stream': PATH_STREAM_Product,
               'Topic': Topic3},
    dag=dag,
)

task4 = PythonOperator(
    task_id='generate_stream4',
    python_callable=generate_stream,
    op_kwargs={'path_stream': PATH_STREAM_Sales,
               'Topic': Topic4},
    dag=dag,
)

task5 = PythonOperator(
    task_id='generate_stream5',
    python_callable=generate_stream,
    op_kwargs={'path_stream': PATH_STREAM_Refund,
               'Topic': Topic5},
    dag=dag,
)



task1                   # set task priority, but here run them in parallel
task2
task3                   # set task priority, but here run them in parallel
task4                  # set task priority, but here run them in parallel
task5