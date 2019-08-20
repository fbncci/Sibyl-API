"""
This module contains the classes that contains methods that can be used for the analysis
"""
from scipy.spatial.distance import euclidean
from dtw import accelerated_dtw


class Similarity:
    """
        This class contains all the method useful for compare two or more signals
    """

    def __init__(self):
        pass

    @staticmethod
    def time_distance(signature, runtime):
        """
            This method allow to compute similarity between 2 signals using
            dinamic time warping.
            Args:
                signature (pandas.DataFrame): signal used as signature for the comparison
                runtime (pandas.DataFrame): signal to compare with the signature
            Returns:
                pandas.DataFrame
        """
        try:
            signature.sort_values(by=["TimeStamp"], inplace=True, ascending=True)
            runtime.sort_values(by=["TimeStamp"], inplace=True, ascending=True)
            result = runtime.copy().tail(1)
            signature = signature["Value"].values
            runtime = runtime["Value"].values
            result_dtw = accelerated_dtw(signature, runtime, dist=euclidean)
            kpi = max(0, 1 - result_dtw[0])
            result["Value"] = kpi
            return result
        except:
            raise
