import logging
import sys
from datetime import datetime, timedelta


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] : %(message)s'
)
logger = logging.getLogger()


def n_days_ago(n):
    return (datetime.today() - timedelta(days=n)).strftime('%Y-%m-%d')


def cli_variables_parser(cli_variables: list = None) -> dict:
    variables = dict()

    if not cli_variables:
        return variables

    for var in cli_variables:
        key, value = var.split("=")
        variables[key] = value
    return variables


sqlbucket_logo = """

           _____ ____    __    ____             __        __ 
          / ___// __ \  / /   / __ )__  _______/ /_____  / /_
          \__ \/ / / / / /   / __  / / / / ___/ //_/ _ \/ __/
         ___/ / /_/ / / /___/ /_/ / /_/ / /__/ ,< /  __/ /_  
        /____/\___\_\/_____/_____/\__,_/\___/_/|_|\___/\__/  

        """


integrity_logo = """

            ____      __                  _ __       
           /  _/___  / /____  ____ ______(_) /___  __
           / // __ \/ __/ _ \/ __ `/ ___/ / __/ / / /
         _/ // / / / /_/  __/ /_/ / /  / / /_/ /_/ / 
        /___/_/ /_/\__/\___/\__, /_/  /_/\__/\__, /  
                           /____/           /____/   

        """

success = """
            
            ___|
                                                   
         \___ \   |   |   __|   __|   _ \   __|   __| 
               |  |   |  (     (      __/ \__ \ \__ \ 
         _____/  \__,_| \___| \___| \___| ____/ ____/ 
                                          
        """
