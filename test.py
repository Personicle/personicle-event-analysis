from utils.config import *
from utils.analysis_requests import get_analysis_requests
from database.write_analysis_to_db import write_analysis_results
from datetime import datetime

from analysis_queries.event_to_event_query import e2e_scatterplot
from analysis_queries.event_to_datastream_query import e2d_scatterplot, run_e2d_analysis
from analysis_queries.datastream_to_event_query import d2e_scatterplot, run_d2e_analysis

if __name__ == "__main__":
    # results = get_analysis_requests()
    # print(results[0])
    # df = e2e_scatterplot(0, 24*60*60, "Strength training", "duration", "Sleep",
    #                      "duration", "00u50rvywd8mGuJw75d7", anchor="CONSEQUENT", aggregation_function="MEAN")
    # df = run_e2d_analysis(0, 24*60*60, "Biking", "Heartrate",
    #                       "heart_rate", "Heartrate", datetime.strptime("2022-06-01", "%Y-%m-%d"), datetime.strptime("2022-12-01", "%Y-%m-%d"), "00u50rvywd8mGuJw75d7", anchor="CONSEQUENT", aggregation_function="MEAN")
    df = run_d2e_analysis(0, 24*60*60, "Sleep", "Heartrate",
                          "heart_rate", "Heartrate", datetime.strptime("2022-01-01", "%Y-%m-%d"), datetime.strptime("2022-12-01", "%Y-%m-%d"), "00u50rvywd8mGuJw75d7", anchor="CONSEQUENT", aggregation_function="MEAN")
    # print(df)
    if df is not None and df.shape[0] > 0:
        # pass
        print(df['correlation_result'].iloc[0])
        # write_analysis_results("00u50rvywd8mGuJw75d7",
        #                        df['correlation_result'].iloc[0], "2be83768-fdf2-4cef-860e-b0c56f845216")
    else:
        print("Empty result")

# "cae1ec8f-e648-4993-ad80-f63f813a0ddc"	"test_user"	"CONSEQUENT"	"RUNNING"	"personal_events"	"duration"	"SLEEP"	"personal_events"	"duration"	"SUM"	"EVENT"	"EVENT"			"(0,24,H)"
# "4ac788f5-e547-429f-9173-5e907ac7b726"	"test_user"	"CONSEQUENT"	"STEPS"	"interval_step_count"		"SLEEP"	"personal_events"	"duration"	"SUM"	"DATASTREAM"	"EVENT"		"DAILY"	"(0,24,H)"
# "2be83768-fdf2-4cef-860e-b0c56f845216"	"00u50rvywd8mGuJw75d7"	"ANTECEDENT"	"Biking"	"test"	"duration"	"heartrate"	"test"		"SUM"	"EVENT"	"DATASTREAM"	"HOURLY"		"(0,12,hours)"
# "64ddb70b-c764-43dd-b151-9fb55a0f5abb"	"00u50rvywd8mGuJw75d7"	"ANTECEDENT"	"Evening Run"	"test"	"duration"	"Afternoon Run"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(6,10,hours)"
# "110651d0-6910-4439-8cce-0dbaef449534"	"00u50rvywd8mGuJw75d7"	"ANTECEDENT"	"Mindfulness"	"test"	"duration"	"Evening Run"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(4,26,minutes)"
# "cd6a4d56-470c-4c5f-829a-954a40f74f37"	"00u50rvywd8mGuJw75d7"	"CONSEQUENT"	"Evening Run"	"test"	"duration"	"Strength training"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(22,,minutes)"
# "07ef535d-da14-4a93-87d1-82f82b2e3909"	"00u50rvywd8mGuJw75d7"	"CONSEQUENT"	"Biking"	"test"	"duration"	"Sleep"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(0,24,hours)"
# "8f205470-6c10-47a0-b033-ddd6430c7df0"	"00u50rvywd8mGuJw75d7"	"ANTECEDENT"	"Sleep"	"test"	"duration"	"Running"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(0,18,hours)"
# "805a4bfb-300d-4e57-bd6f-2243fd1de971"	"00u50rvywd8mGuJw75d7"	"CONSEQUENT"	"Strength training"	"test"	"duration"	"Sleep"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(0,24,hours)"
