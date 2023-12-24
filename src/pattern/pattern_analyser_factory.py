from pandas.core.frame import DataFrame

from constant.filter.pattern import Pattern

from pattern.pattern_analyser import PatternAnalyser
from pattern.unusual_volume_ramp_up import UnusualVolumeRampUp
from pattern.initial_pop_up import InitialPopUp
from pattern.closest_to_new_high_or_new_high import ClosestToNewHighOrNewHigh
from pattern.new_five_minute_candle_high import NewFiveMinuteCandleHigh

class PatternAnalyserFactory:
    @staticmethod
    def get_pattern_analyser(analyser: Pattern, historical_data_df: DataFrame, ticker_to_contract_dict: dict) -> PatternAnalyser:
        if Pattern.INITIAL_POP_UP == analyser:
            return InitialPopUp(historical_data_df, ticker_to_contract_dict)
        elif Pattern.UNUSUAL_VOLUME_RAMP_UP == analyser:
            return UnusualVolumeRampUp(historical_data_df, ticker_to_contract_dict)
        elif Pattern.CLOSEST_TO_NEW_HIGH_OR_NEW_HIGH == analyser:
            return ClosestToNewHighOrNewHigh(historical_data_df, ticker_to_contract_dict)
        elif Pattern.NEW_FIVE_MINUTE_CANDLE_HIGH == analyser:
            return NewFiveMinuteCandleHigh(historical_data_df, ticker_to_contract_dict)
        else:
            raise Exception(f'Pattern analyser of {analyser} not found')