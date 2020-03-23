import requests
from bs4 import BeautifulSoup
import datetime
import time
from arcgis.gis import GIS
from arcgis import features
import pandas as pd
from copy import deepcopy


class Maine(object):
    def __init__(self):
        self.gis = GIS("https://nitro.maps.arcgis.com", "joel_Nitro", "...")

    def pull_data(self):
        # Collect and parse first page
        page = requests.get('https://www.maine.gov/dhhs/mecdc/infectious-disease/epi/airborne/coronavirus.shtml')
        soup = BeautifulSoup(page.text, 'html.parser')

        tables = soup.find_all(class_='travelAdvisories')
        if not tables:
            raise Exception("No 'travelAdvisories' tables found")

        # get totals table
        table = tables[0]
        table_rows = table.find('tbody').find_all('tr')
        advisory = table.thead.find(class_='advisory')
        update_time = table.thead.find(class_='advisoryDt')

        if None in [advisory, update_time] or advisory.text != "Maine COVID-19  Testing Data":
            raise Exception("No 'Maine COVID-19 Testing Data' found")

        data = []
        for tr in table_rows[1:]:
            td = tr.find_all('td')
            row = [i.text.replace('\\xa0', '0').replace(',', '') for i in td]
            data.append(row)

        totals_df = pd.DataFrame(data, columns=["ConfirmedCases", "NegativeCases"])
        self.update_total_cases(totals_df, update_time.text)

        # get cases table
        table = tables[1]
        table_rows = table.find('tbody').find_all('tr')
        advisory_updated = table.thead.find(class_='advisoryDt')

        if None in [advisory_updated] or advisory_updated.text != "Confirmed and Recovered Case Counts by County":
            raise Exception("No 'Confirmed and Recovered Case Counts by County' cases found")

        data = []
        for tr in table_rows[1:]:
            td = tr.find_all('td')
            row = [i.text.replace('\xa0', '0').replace(',', '') for i in td]
            data.append(row)

        cases_df = pd.DataFrame(data, columns=["County", "ConfirmedCases", "Recovered"])
        self.update_county_cases(cases_df, update_time.text)
        self.update_daily_cases(totals_df, cases_df, update_time.text)

    def update_total_cases(self, with_data_frame, update_time):
        layer = self.gis.content.get("a2e8fb4b5f7948908427d26d23167c26").layers[0]
        feature_set = layer.query()
        feature_set.sdf.head()
        all_features = feature_set.features

        matching_row = with_data_frame.iloc[0]
        original_feature = [f for f in all_features if f.attributes['OBJECTID'] == 1][0]
        feature_to_be_updated = deepcopy(original_feature)

        # assign the updated values
        feature_to_be_updated.attributes['ConfirmedCases'] = int(matching_row['ConfirmedCases'])
        feature_to_be_updated.attributes['PresumptiveCases'] = int(0)
        feature_to_be_updated.attributes['NegativeCases'] = int(matching_row['NegativeCases'])
        feature_to_be_updated.attributes['Updated'] = str(update_time)

        layer.edit_features(updates=[feature_to_be_updated])

    def update_county_cases(self, with_data_frame, update_time):
        layer = self.gis.content.get("b672bc7ee7064f66bb7f0c87ec466620").layers[0]
        feature_set = layer.query()
        feature_set.sdf.head()
        all_features = feature_set.features

        features_to_be_updated = []

        for original_feature in all_features:
            matching_row = with_data_frame.where(with_data_frame.County == original_feature.attributes['COUNTY']).dropna().iloc[0]
            feature_to_be_updated = deepcopy(original_feature)

            # assign the updated values
            feature_to_be_updated.attributes['ConfirmedCases'] = 0 if (matching_row['ConfirmedCases'] == '') else int(matching_row['ConfirmedCases'])
            feature_to_be_updated.attributes['PresumptiveCases'] = int(0)
            feature_to_be_updated.attributes['NegativeCases'] = 0 if (matching_row['Recovered'] == '') else int(matching_row['Recovered'])
            feature_to_be_updated.attributes['Updated'] = str(update_time)

            features_to_be_updated.append(feature_to_be_updated)

        layer.edit_features(updates=features_to_be_updated)

    def update_daily_cases(self, totals_data_frame, cases_data_frame, update_time):
        table = self.gis.content.get("993203d373a44894a36588c4b797ffa3").tables[0]
        record_set = table.query()
        record_set.sdf.head()
        last_record = record_set.features[-1:]
        second_to_last_record = record_set.features[-2:]
        latest_totals = totals_data_frame.iloc[0]

        delta_confirmed_presumptive = int(0)
        delta_recovered = int(0)
        latest_recovered = int(0)
        for index, row in cases_data_frame.iterrows():
            latest_recovered += 0 if (row['Recovered'] == '') else int(row['Recovered'])

        # add new if next day
        if last_record and str(last_record[0].attributes['ReportDate']) != datetime.datetime.now().strftime("%Y/%m/%d") and str(last_record[0].attributes['ReportDateString']) != str(update_time):
            # calculate deltas from day befores record
            print("ADD -- last record is NOT today and latest update time is new")
            last_confirmed_presumptive = int(last_record[0].attributes['Total_Confirmed']) + int(last_record[0].attributes['Total_Presumptive'])
            latest_confirmed_presumptive = int(latest_totals['ConfirmedCases']) + int(0)

            delta_confirmed_presumptive = latest_confirmed_presumptive - last_confirmed_presumptive
            last_recovered = int(last_record[0].attributes['Total_Recovered'])
            delta_recovered = latest_recovered - last_recovered

            record_to_be_added = features.Feature(
                attributes={
                    "ReportDate": datetime.datetime.now().strftime("%Y/%m/%d"),
                    "Total_Confirmed": int(latest_totals['ConfirmedCases']),
                    "Total_Presumptive": int(0),
                    "Total_Confirmed_Presumptive": int(latest_totals['ConfirmedCases']) + int(0),
                    "Total_Negative": int(latest_totals['NegativeCases']),
                    "Total_Recovered": latest_recovered,
                    "ReportDateString": str(update_time),
                    "Delta_Confirmed_Presumptive": int(delta_confirmed_presumptive),
                    "Delta_Recovered": int(delta_recovered),
                    "Delta_Negative": int(latest_totals['NegativeCases']) - int(last_record[0].attributes['Total_Negative'])
                }
            )
            table.edit_features(adds=[record_to_be_added])
        # update if same day and different time
        elif last_record and second_to_last_record and str(last_record[0].attributes['ReportDate']) == datetime.datetime.now().strftime("%Y/%m/%d") and str(last_record[0].attributes['ReportDateString']) != str(update_time):
            print("UPDATE -- last record is today and latest update time is new")
            record_to_be_updated = deepcopy(last_record[0])

            last_confirmed_presumptive = int(second_to_last_record[0].attributes['Total_Confirmed']) + int(
                second_to_last_record[0].attributes['Total_Presumptive'])
            latest_confirmed_presumptive = int(latest_totals['ConfirmedCases']) + int(
                0)

            delta_confirmed_presumptive = latest_confirmed_presumptive - last_confirmed_presumptive
            last_recovered = int(second_to_last_record[0].attributes['Total_Recovered'])
            delta_recovered = latest_recovered - last_recovered

            record_to_be_updated.attributes["ReportDate"] = datetime.datetime.now().strftime("%Y/%m/%d")
            record_to_be_updated.attributes["Total_Confirmed"] = int(latest_totals['ConfirmedCases'])
            record_to_be_updated.attributes["Total_Presumptive"] = int(0)
            record_to_be_updated.attributes["Total_Confirmed_Presumptive"] = int(latest_totals['ConfirmedCases']) + int(0)
            record_to_be_updated.attributes["Total_Negative"] = int(latest_totals['NegativeCases'])
            record_to_be_updated.attributes["Total_Recovered"] = latest_recovered
            record_to_be_updated.attributes["ReportDateString"] = str(update_time)
            record_to_be_updated.attributes["Delta_Confirmed_Presumptive"] = int(delta_confirmed_presumptive)
            record_to_be_updated.attributes["Delta_Recovered"] = int(delta_recovered)
            record_to_be_updated.attributes["Delta_Negative"] = int(latest_totals['NegativeCases']) - int(second_to_last_record[0].attributes['Total_Negative'])

            table.edit_features(updates=[record_to_be_updated])


    def notify_script_exception(self, with_warning):
        dt = datetime.datetime.now()
        print("Subject: WARNING", "\nDate: ", dt, "\nException: ", with_warning)


def main():
    delta_hour = 0
    maine = Maine()
    while True:
        now_hour = datetime.datetime.now().hour

        if delta_hour != now_hour:
            try:
                maine.pull_data()
            except Exception as exception:
                maine.notify_script_exception(exception)
            print("Script finished running... ", datetime.datetime.now())
            delta_hour = now_hour

        time.sleep(1800)  # 60 seconds


main()
