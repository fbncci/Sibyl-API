"""
This module contains the class TimeSeries che allow to analyze
timeseries data in order to detect trends
"""
import math
import pandas as pd
import numpy as np
from scipy import stats


class TimeSeries:
    """
    This class contains all the method useful for the analysis of Timeseries data.
    """

    def __init__(self):
        pass

    def calculate_cl(self, data, remove_outliers):
        """ Function used to compute the control limits

            Args:
                data (pandas.DataFrame): the raw data
                remove_outliers (int): represent the confidence level

            Returns:
                pandas.DataFrame: 5 control limits (UCL, UWL, MEAN, LWL, LCL)
                with machine informations
        """
        try:
            value = data["Value"]
            value = self.remove_outliers(value, remove_outliers).reset_index(drop=True)
            result = self.cl_definition(value, 4)
            result["IdMachine"] = data["IdMachine"][0]
            result["IdSig"] = data["IdSig"][0]
            result["TimeStamp"] = data.tail(1)["TimeStamp"].values[0]
            return result
        except:
            raise

    @staticmethod
    def remove_outliers(value, remove_outlier):
        """ Function used when a message with topic sibyl is received.

            Args:
                value (pandas.Series): the raw data
                remove_outlier (int): represent the confidence level

            Returns:
                numpy.array: the signal without outlier
        """
        try:
            if value.shape[0] > 0:
                percent = remove_outlier
                q75, q25 = np.nanpercentile(
                    value, [percent, 100 - percent], interpolation="linear"
                )
                iqr = q75 - q25
                value = value[value >= (q25 - 1.5 * iqr)]
                value = value[value <= (q75 + 1.5 * iqr)]
                value.reset_index(drop=True)
            return value
        except:
            raise

    @staticmethod
    def cl_definition(value, k):
        """ Function used to compute the control limits

            Args:
                value (pandas.Series): the raw data
                k (float): how much large the bands

            Returns:
                pandas.DataFrame: 5 control limits (UCL, UWL, MEAN, LWL, LCL)
        """
        try:
            m_val = value.mean(skipna=True)
            sd_val = value.std(skipna=True)
            lwl = np.nanpercentile(value, 1)
            uwl = np.nanpercentile(value, 99)
            ucl = m_val + k * sd_val
            lcl = m_val - k * sd_val
            lcl, lwl, uwl, ucl = np.sort([lcl, lwl, uwl, ucl])
            if ucl <= 10 ** -8 and lcl >= -10 ** -8:
                ucl = 0.01
                lcl = -0.01
            elif (uwl - lwl) <= 10 ** -8:  # alternatively (UWL - LWL)/M
                lwl = max(m_val - k * sd_val, m_val - np.abs(m_val) * 0.01)
                uwl = min(m_val + k * sd_val, m_val + np.abs(m_val) * 0.01)
                ucl = max(m_val + k * sd_val, m_val + np.abs(m_val) * 0.01)
                lcl = min(m_val - k * sd_val, m_val - np.abs(m_val) * 0.01)
            param = [
                ["UCL", ucl],
                ["UWL", uwl],
                ["MEAN", m_val],
                ["LWL", lwl],
                ["LCL", lcl],
            ]
            output = pd.DataFrame(param, columns=["Kind", "Value"])
            return output
        except:
            raise

    def run_cl_analysis(self, data, c_l, param):
        """ Function used to compute the control limits

            Args:
                data (pandas.DataFrame): the raw data
                c_l (pandas.DataFrame): the control limits to use for the analysis
                param (dict): contains the parameters useful for the functions

            Returns:
                pandas.DataFrame: indicators that gives informations about anomalies
        """
        try:
            remove_outliers = param["remove_outliers_run"]
            data = data.sort_values(by=["TimeStamp"], ascending=True)
            value = data["Value"]
            value = self.remove_outliers(value, remove_outliers).reset_index(drop=True)
            return self.analysis(value, c_l, param)
        except:
            raise

    def analysis(self, value, c_l, param):
        """ Function used to compute the control limits

            Args:
                value (pandas.Series): the raw data
                c_l (pandas.DataFrame): the control limits to use for the analysis
                param (dict): contains the parameters useful for the functions

            Returns:
                pandas.DataFrame: indicators that gives informations about anomalies
        """
        try:
            alpha, slope = self.trend_fun(value, c_l, 0.1)
            # param_anal = beta, over_uwl, over_ucl
            param_anal = self.control_chart_analysis(value, c_l, param)  # type: tuple
            # param_anal_inf = beta_inf, over_uwl_inf, over_ucl_inf
            param_anal_inf = self.control_chart_analysis(
                value, c_l, param, "lower"
            )  # type: tuple
            gamma = 1 - self.saturation(
                (
                    value.mean(skipna=True)
                    - c_l.loc[c_l["Kind"] == "MEAN", "Value"].values[0]
                )
                / (
                    c_l.loc[c_l["Kind"] == "UCL", "Value"].iloc[0]
                    - c_l.loc[c_l["Kind"] == "LCL", "Value"].values[0]
                ),
                param,
            )
            mean = value.mean(skipna=True)
            std_dev = value.std(skipna=True)
            result = [
                ["alpha", alpha],
                ["slope", slope],
                ["beta", param_anal[0]],
                ["beta_inf", param_anal_inf[0]],
                ["OverUWLInf", param_anal_inf[1]],
                ["OverUCLInf", param_anal_inf[2]],
                ["OverUWL", param_anal[1]],
                ["OverUCL", param_anal[2]],
                ["Gamma", gamma],
                ["Mean", mean],
                ["StdDev", std_dev],
            ]
            result = pd.DataFrame(result, columns=["Kind", "Value"])
            return result
        except:
            raise

    @staticmethod
    def saturation(val, param):
        """ Function used to compute...

            Args:
                val (float): the raw data
                param (dict): contains the parameters useful for the functions

            Returns:
                float:
        """
        try:
            type_analysis = param["type_analysis"]
            if type_analysis == "both":
                if val <= 0:
                    sat = math.exp(val)
                else:
                    sat = math.exp(-val)

            elif type_analysis == "lower":
                if val <= 0:
                    sat = math.exp(val)
                else:
                    sat = 1
            else:
                if val <= 0:
                    sat = 1
                else:
                    sat = math.exp(-val)

            return sat
        except:
            raise

    def trend_fun(self, value, c_l, pval):
        """ Function used to compute the trend see trend_fun() below for more details.

            Args:
                value (pandas.Series): the raw data
                c_l (pandas.DataFrame): the control limits to use for the analysis
                pval (float): confidence level in probability

            Returns:
                float: angle
                float: slope
        """
        try:
            slope = self.trend(value, pval)

            angle = (
                math.atan(
                    slope
                    / (
                        c_l.loc[c_l["Kind"] == "UCL", "Value"].values[0]
                        - c_l.loc[c_l["Kind"] == "LCL", "Value"].values[0]
                    )
                )
                / np.pi
                * 2
            )
            return [angle, slope]
        except:
            raise

    @staticmethod
    def trend(value, pval):
        """ Function used to compute the Theil-Sen estimator for a set of points (x, y).
            Theilslopes implements a method for robust linear regression.
            It computes the slope as the median of all slopes between paired values.

            link: https://docs.scipy.org/doc/scipy-0.17.1/reference/generated/scipy.stats.theilslopes.html

            Args:
                value (pandas.Series): the raw data
                pval (float): confidence level in probability (default is 0.95 or 95%)

            Returns:
                float: slope
        """
        try:
            if len(value) <= 1000:
                # res = slope, intercept, lo_slope, up_slope
                res = stats.theilslopes(value, np.arange(len(value)), 1 - pval)
                if res[2] < 0 < res[3]:
                    slope = 0
                else:
                    slope = res[0]
            elif 1000 < len(value) <= 1000000:
                # dimension of the batch
                dim_batch = int(len(value) / 1000)
                # number of batches
                n_batch = int(len(value) / dim_batch)
                # new list of values batched
                value_batch = []
                for i in range(0, n_batch):
                    value_batch.append(
                        np.median(value[dim_batch * i : dim_batch * (i + 1)])
                    )
                res = stats.theilslopes(
                    value_batch, np.arange(len(value_batch)), 1 - pval
                )
                if res[2] < 0 < res[3]:
                    slope = 0
                else:
                    slope = res[0]
            else:
                y_list = value.tolist()
                x_list = np.arange(len(y_list))
                # lsq_res = slope, intercept, r_value, p_value, std_err
                lsq_res = stats.linregress(x_list, y_list)
                if pval < lsq_res[3]:
                    slope = 0
                else:
                    slope = lsq_res[0]

            return slope
        except:
            raise

    @staticmethod
    def control_chart_analysis(value, c_l, param, anal_type=None):
        """ Function used to compute...

            Args:
                value (pandas.Series): the raw data
                c_l (pandas.DataFrame): the control limits to use for the analysis
                param (dict): contains the parameters useful for the functions
                anal_type (str): represent if consider lower bands or upper bands

            Returns:
                float: beta
                float: len_w
                float: len_c
        """
        try:
            w_w = param["ww"]
            w_c = param["wc"]
            # val = np.array(value, dtype=pd.Series)
            if anal_type == "lower":
                lwl = c_l.loc[c_l["Kind"] == "LWL", "Value"].values[0]
                lcl = c_l.loc[c_l["Kind"] == "LCL", "Value"].values[0]
                len_w = len(value[(value < lwl) & (value >= lcl)])
                len_c = len(value[(value < lcl)])
            else:
                uwl = c_l.loc[c_l["Kind"] == "UWL", "Value"].values[0]
                ucl = c_l.loc[c_l["Kind"] == "UCL", "Value"].values[0]
                len_w = len(value[(value > uwl) & (value <= ucl)])
                len_c = len(value[(value > ucl)])
            beta = (w_w * len_w + w_c * len_c) / (max(w_w, w_c) * len(value))
            return [beta, len_w / len(value), len_c / len(value)]
        except:
            raise

    def define_cusum(self, data, remove_outliers):
        """ Function used to define the parameter useful for the cusum algorithm

            Args:
                data (pandas.DataFrame): the raw data
                remove_outliers (int): represent the confidence level

            Returns:
                pandas.DataFrame: 2 bands and initialization error cusum(LB, UB, SP, SN)
        """
        try:
            value = data["Value"]
            value = self.remove_outliers(value, remove_outliers).reset_index(drop=True)
            result = self.cl_definition(value, 1.5)
            result = result.loc[
                (result["Kind"] == "UCL") | (result["Kind"] == "LCL"), :
            ]
            result["IdMachine"] = data["IdMachine"][0]
            result["IdSig"] = data["IdSig"][0]
            result["TimeStamp"] = data.tail(1)["TimeStamp"].values[0]
            return result
        except:
            raise

    @staticmethod
    def run_cusum(data, old_values):
        """ Function used to compute the cusum

            Args:
                data (pandas.DataFrame): the raw data
                old_values (pandas.DataFrame): contains the bands and the previous values for errors

            Returns:
                pandas.DataFrame: current values of the errors computed in the cusum algorithm
        """
        try:
            value = data["Value"]
            lcl = old_values.loc[old_values["Kind"] == "LCL", "Value"].values[0]
            ucl = old_values.loc[old_values["Kind"] == "UCL", "Value"].values[0]
            sum_positive = old_values.loc[old_values["Kind"] == "SP", "Value"].values[0]
            sum_negative = old_values.loc[old_values["Kind"] == "SN", "Value"].values[0]
            differences_pos = (value - ucl) / (ucl - lcl)
            differences_neg = (lcl - value) / (ucl - lcl)
            sum_positive = max(0, sum_positive + differences_pos.sum())
            sum_negative = max(0, sum_negative + differences_neg.sum())
            result = [
                ["SP", sum_positive],
                ["SN", sum_negative],
                ["LCL", lcl],
                ["UCL", ucl],
            ]
            output = pd.DataFrame(result, columns=["Kind", "Value"])
            return output
        except:
            raise
