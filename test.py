from utils.config import *
from utils.analysis_requests import get_analysis_requests
from database.write_analysis_to_db import write_analysis_results

from analysis_queries.event_to_event_query import e2e_scatterplot
if __name__ == "__main__":

    # results = get_analysis_requests()
    # print(results[0])
    df = e2e_scatterplot(0, 24*60*60, "Biking", "duration", "Sleep",
                         "duration", "00u50rvywd8mGuJw75d7")
    print(df)
    if df is not None and df.shape[0] > 0:
        print(df['correlation_result'].iloc[0])
        write_analysis_results("00u50rvywd8mGuJw75d7",
                               df['correlation_result'].iloc[0], "07ef535d-da14-4a93-87d1-82f82b2e3909")
    else:
        print("Empty result")

# "cae1ec8f-e648-4993-ad80-f63f813a0ddc"	"test_user"	"CONSEQUENT"	"RUNNING"	"personal_events"	"duration"	"SLEEP"	"personal_events"	"duration"	"SUM"	"EVENT"	"EVENT"			"(0,24,H)"
# "4ac788f5-e547-429f-9173-5e907ac7b726"	"test_user"	"CONSEQUENT"	"STEPS"	"interval_step_count"		"SLEEP"	"personal_events"	"duration"	"SUM"	"DATASTREAM"	"EVENT"		"DAILY"	"(0,24,H)"
# "2be83768-fdf2-4cef-860e-b0c56f845216"	"00u50rvywd8mGuJw75d7"	"ANTECEDENT"	"Biking"	"test"	"duration"	"heartrate"	"test"		"SUM"	"EVENT"	"DATASTREAM"	"HOURLY"		"(0,12,hours)"
# "64ddb70b-c764-43dd-b151-9fb55a0f5abb"	"00u50rvywd8mGuJw75d7"	"ANTECEDENT"	"Evening Run"	"test"	"duration"	"Afternoon Run"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(6,10,hours)"
# "110651d0-6910-4439-8cce-0dbaef449534"	"00u50rvywd8mGuJw75d7"	"ANTECEDENT"	"Mindfulness"	"test"	"duration"	"Evening Run"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(4,26,minutes)"
# "cd6a4d56-470c-4c5f-829a-954a40f74f37"	"00u50rvywd8mGuJw75d7"	"CONSEQUENT"	"Evening Run"	"test"	"duration"	"Strength training"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(22,,minutes)"
# "07ef535d-da14-4a93-87d1-82f82b2e3909"	"00u50rvywd8mGuJw75d7"	"CONSEQUENT"	"Biking"	"test"	"duration"	"Sleep"	"test"	"duration"	"SUM"	"EVENT"	"EVENT"			"(0,24,hours)"
