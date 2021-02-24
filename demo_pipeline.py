import os

from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2))
def demo_pipeline():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """

    @task()
    def raw_to_edf():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        # it is not ideal to read from file
         
        path = Variable.get('path')
        input_file = os.path.join(path, 'raw_input.txt')
        output_file = os.path.join(path, 'edf_output.txt')

        with open(input_file, "r") as in_file:
            line = in_file.read()

        with open(output_file, "w") as out_file:
            out_file.write(line + "World!!!")

        return output_file

    @task()
    def sleep(edf_file):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        path = Variable.get('path')
        output_path = os.path.join(path, 'sleep_output.txt')
        with open(edf_file, "r") as in_file:
            line = in_file.read()

        with open(output_path, "w") as out_file:
            out_file.write(line + " AND SLEEEP!!!")

        return output_path
        # test
    @task()
    def gait(edf_file):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        from macro_gait.WalkingBouts import WalkingBouts
        start_time = '2019-08-22 10:05:16'
        duration = 1000
        l_file = r'/Users/matthewwong/Documents/coding/nimbal/data/OND06_SBH_2891_GNAC_ACCELEROMETER_LAnkle.edf'
        r_file = r'/Users/matthewwong/Documents/coding/nimbal/data/OND06_SBH_2891_GNAC_ACCELEROMETER_RAnkle.edf'
        WalkingBouts(l_file, r_file, start_time=start_time, duration_sec=duration)
        
        path = Variable.get('path')
        output_path = os.path.join(path, 'gait_output.txt')
        with open(edf_file, "r") as in_file:
            line = in_file.read()

        with open(output_path, "w") as out_file:
            out_file.write(line + " AND GAIT!!!")

        return output_path

    @task()
    def nonwear(edf_file):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        path = Variable.get('path')
        output_path = os.path.join(path, 'nw_output.txt')
        with open(edf_file, "r") as in_file:
            line = in_file.read()

        with open(output_path, "w") as out_file:
            out_file.write(line + " AND NONWEAR!!!")

        return output_path

    @task()
    def feedback_form(gait_file, sleep_file, nonwear_file):
        path = Variable.get('path')
        output_path = os.path.join(path, 'feedback_output.txt')

        with open(gait_file, "r") as in_file:
            gait_line = in_file.read()

        with open(sleep_file, "r") as in_file:
            sleep_line = in_file.read()

        with open(nonwear_file, "r") as in_file:
            nonwear_line = in_file.read()

        with open(output_path, "w") as out_file:
            out_file.write(gait_line + sleep_line + nonwear_line)

        return output_path
    
    requirements = Variable.get('requirements').replace('\n', ' ').replace('\r', ' ')
    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command=f'python -m pip install -I {requirements}',
    )
    
    edf_file = raw_to_edf()
    gait_file = gait(edf_file)
    sleep_file = sleep(edf_file)
    nonwear_file = nonwear(edf_file)
    feedback_form(gait_file, sleep_file, nonwear_file)
    install_deps >> edf_file

etl_dag = demo_pipeline()

