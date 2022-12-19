import pandas as pd


class M3CData:

    sheet_names = ['M3Year', 'M3Month', 'M3Quart', 'M3Other']

    def __init__(self):
        self.__data = pd.read_excel('M3C.xls', sheet_name=self.sheet_names)
        self.data_dict = {}
        self.__process()

    def __process_year(self, series):
        series_name = series.iloc[0].replace(' ', '')
        series_length = series.iloc[1]
        series_category = series.iloc[3].strip()
        series_data = series.iloc[6:].dropna().values
        series_index = pd.date_range(
            start=f"{series.iloc[4]}/{series.iloc[5]}", periods=series_length, freq='YS')
        series_index.name = 'id'
        return {'series_name': series_name, 'series_category': series_category, 'series_df': pd.DataFrame(series_data, index=series_index, columns=[series_name])}

    def __process_month(self, series):
        series_name = series.iloc[0].replace(' ', '')
        series_length = series.iloc[1]
        series_category = series.iloc[3].strip()
        series_data = series.iloc[6:].dropna().values
        if series.iloc[4] != 0:  # has time index
            series_index = pd.date_range(
                start=f"{series.iloc[4]}/{series.iloc[5]}", periods=series_length, freq='MS')
        else:
            series_index = pd.RangeIndex(start=0, stop=series_length, step=1)
        series_index.name = 'id'
        return {'series_name': series_name, 'series_category': series_category, 'series_df': pd.DataFrame(series_data, index=series_index, columns=[series_name])}

    def __process_quart(self, series):
        series_name = series.iloc[0].replace(' ', '')
        series_length = series.iloc[1]
        series_category = series.iloc[3].strip()
        series_data = series.iloc[6:].dropna().values
        series_index = pd.date_range(
            start=f"{series.iloc[4]}/{series.iloc[5]}", periods=series_length, freq='QS')
        series_index.name = 'id'
        return {'series_name': series_name, 'series_category': series_category, 'series_df': pd.DataFrame(series_data, index=series_index, columns=[series_name])}

    def __process_other(self, series):
        series_name = series.iloc[0].replace(' ', '')
        series_category = series.iloc[3].strip()
        series_data = series.iloc[6:].dropna().values
        series_df = pd.DataFrame(series_data, columns=[series_name])
        series_df.index.name = 'id'
        return {'series_name': series_name, 'series_category': series_category, 'series_df': series_df}

    def __process(self):

        for period in self.sheet_names:
            cdf = self.__data[period]
            series_count = cdf.shape[0]
            for i in range(series_count):
                if period == 'M3Year':
                    current_row_data = self.__process_year(cdf.iloc[i])
                    self.data_dict[current_row_data['series_name']
                                   ] = current_row_data['series_df']
                elif period == 'M3Month':
                    current_row_data = self.__process_month(cdf.iloc[i])
                    self.data_dict[current_row_data['series_name']
                                   ] = current_row_data['series_df']
                elif period == 'M3Quart':
                    current_row_data = self.__process_quart(cdf.iloc[i])
                    self.data_dict[current_row_data['series_name']
                                   ] = current_row_data['series_df']
                elif period == 'M3Other':
                    current_row_data = self.__process_other(cdf.iloc[i])
                    self.data_dict[current_row_data['series_name']
                                   ] = current_row_data['series_df']

    def save_series_to_csv(self, series_name):
        self.data_dict[series_name].to_csv(f'output/{series_name}.csv')

    def save_all_series_to_csv(self):
        for series_name in self.data_dict.keys():
            self.save_series_to_csv(series_name)
