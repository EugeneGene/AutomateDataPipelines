import pendulum
import logging

from airflow.decorators import dag, task

@dag(
    schedule_interval='@hourly',
    start_date=pendulum.now()
)
def task_dependencies():

    @task()
    def hello_world():
        logging.info("Hello World")

    @task()
    def addition(first,second):
        logging.info(f"{first} + {second} = {first+second}")
        return first+second

    @task()
    def subtraction(first,second):
        logging.info(f"{first -second} = {first-second}")
        return first-second

    @task()
    def division(first,second):
        logging.info(f"{first} / {second} = {int(first/second)}")   
        return int(first/second)     

# TODO: call the hello world task function
    hello=hello_world()
# TODO: call the addition function with some constants (numbers)
    two_plus_two=addition(2,2)
# TODO: call the subtraction function with some constants (numbers)
    two_from_six=subtraction(6,2)
# TODO: call the division function with some constants (numbers)
    eight_divided_by_two = division(8,2)
# TODO: create the dependency graph for the first three tasks
    hello >> two_plus_two 
    hello >> two_from_six
    hello >> eight_divided_by_two
# TODO: Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

    sum_divided_by_difference = division(two_plus_two, two_from_six)
    two_plus_two >> sum_divided_by_difference
    two_from_six >> sum_divided_by_difference

#  TODO: assign the result of the addition function to a variable
    nine_plus_three = addition(9,3)
#  TODO: assign the result of the subtraction function to a variable
    nine_minus_three = subtraction(9,3)
#  TODO: pass the result of the addition function, and the subtraction functions to the division function
    sum_divided_by_difference = division(nine_plus_three, nine_minus_three)
#  TODO: create the dependency graph for the last three tasks
    nine_plus_three >> sum_divided_by_difference
    nine_minus_three >> sum_divided_by_difference


task_dependencies_dag=task_dependencies()