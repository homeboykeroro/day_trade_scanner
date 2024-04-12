import pandas as pd
import numpy as np

class FinancialData:
    MAX_DISPLAY_RESULT = 3
    NO_OF_WHITESPACE = 6
    
    def __init__(self, symbol: str, financial_data_dict: dict):
        self.__symbol = symbol
        self.__quarterly_cash_flow_df = financial_data_dict.get('quarterly_cash_flow_df')
        self.__quarterly_balance_sheet_df = financial_data_dict.get('quarterly_balance_sheet_df')
        self.__quarterly_income_stmt_df = financial_data_dict.get('quarterly_income_stmt_df')
        self.__annual_cashflow_df = financial_data_dict.get('annual_cashflow_df')
        self.__annual_balance_sheet_df = financial_data_dict.get('annual_balance_sheet_df')
        self.__annual_income_stmt_df = financial_data_dict.get('annual_income_stmt_df')
        self.__major_holders_df = financial_data_dict.get('major_holders_df')
        self.__institutional_holders_df = financial_data_dict.get('institutional_holders_df')
    
    def __members(self):
        return (self.__symbol, self.__quarterly_cash_flow_df, self.__quarterly_balance_sheet_df, self.__quarterly_income_stmt_df, self.__annual_cashflow_df, self.__annual_balance_sheet_df, self.__annual_income_stmt_df, self.__major_holders_df, self.__institutional_holders_df)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FinancialData):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())
    
    def __get_max_str_len(self, value_list: list):
        max_val_len = 0
        
        for val in value_list:
            if len(val) > max_val_len:
                max_val_len = len(val)
                
        return max_val_len

    def __get_concat_str(self, max_str_len: int, val_list: list):
        concat_str = ''
        for val in val_list:
            concat_str += val
            val_len = len(val)

            for _ in range(max_str_len - val_len):
                concat_str += ' '
                
        return concat_str

    def __convert_date_list_to_str(self, date_list: list):
        date_str_list = []
        
        for dt in date_list:
            display_datetime_str = ''
            
            try:
                display_datetime_str = dt.strftime('%Y-%m-%d')
            except ValueError:
                display_datetime_str = str(dt)
            
            date_str_list.append(display_datetime_str)
            
        return date_str_list
    
    def __convert_value_list(self, value_list: list):
        str_value_list = []
        
        for val in value_list:
            if np.isnan(val):
                str_value_list.append('NaN')
            elif val > 0:
                str_value_list.append(f'${"{:,}".format(int(val))}')
            else:
                str_value_list.append(f'-${"{:,}".format(int(abs(val)))}')
        
        return str_value_list
    
    def add_financials_to_embed_msg(self, embed):
        # Cashflow
        display_cashflow_df = self.__quarterly_cash_flow_df if not self.__quarterly_cash_flow_df.empty else self.__annual_cashflow_df
        cashflow_date_list = display_cashflow_df.columns.tolist()
        cashflow_date_str_list = self.__convert_date_list_to_str(cashflow_date_list)[:self.MAX_DISPLAY_RESULT]
        cashflow_value_list = self.__convert_value_list(display_cashflow_df.values[0].tolist())[:self.MAX_DISPLAY_RESULT]
        
        check_cashflow_len_list = cashflow_date_str_list + cashflow_value_list
        max_cashflow_word_len = self.__get_max_str_len(check_cashflow_len_list)
        max_cashflow_word_len += self.NO_OF_WHITESPACE
        
        cashflow_date_display = self.__get_concat_str(max_cashflow_word_len, cashflow_date_str_list)
        cashflow_value_display = self.__get_concat_str(max_cashflow_word_len, cashflow_value_list)
        embed.add_field(name = f'\n\nCashflow Data: \n{cashflow_date_display}\n{cashflow_value_display}', value='\u200b', inline = False)
        
        #Debts
        display_debt_df = self.__quarterly_balance_sheet_df.loc[['Total Debt']] if not self.__quarterly_balance_sheet_df.empty else self.__annual_balance_sheet_df.loc[['Total Debt']]
        debt_date_list = display_debt_df.columns.tolist()
        debt_date_str_list = self.__convert_date_list_to_str(debt_date_list)[:self.MAX_DISPLAY_RESULT]
        debt_value_list = self.__convert_value_list(display_debt_df.values[0].tolist()[:self.MAX_DISPLAY_RESULT])
        
        check_debt_len_list = debt_date_str_list + debt_value_list
        max_debts_word_len = self.__get_max_str_len(check_debt_len_list)
        max_debts_word_len += self.NO_OF_WHITESPACE
        
        debt_date_display = self.__get_concat_str(max_debts_word_len, debt_date_str_list)
        debt_value_display = self.__get_concat_str(max_debts_word_len, debt_value_list)
        embed.add_field(name = f'Debt Data: \n{debt_date_display}\n{debt_value_display}', value='\u200b', inline = False)
        
        # Assests
        display_assests_df = self.__quarterly_balance_sheet_df.loc[['Total Assets']] if not self.__quarterly_balance_sheet_df.empty else self.__annual_balance_sheet_df.loc[['Total Assets']]
        assests_date_list = display_assests_df.columns.tolist()
        assests_date_str_list = self.__convert_date_list_to_str(assests_date_list)[:self.MAX_DISPLAY_RESULT]
        assests_value_list = self.__convert_value_list(display_assests_df.values[0].tolist())[:self.MAX_DISPLAY_RESULT]
        
        check_assests_len_list = assests_date_str_list + assests_value_list
        max_assests_word_len = self.__get_max_str_len(check_assests_len_list)
        max_assests_word_len += self.NO_OF_WHITESPACE
        
        assests_date_display = self.__get_concat_str(max_assests_word_len, assests_date_str_list)
        assests_value_display = self.__get_concat_str(max_assests_word_len, assests_value_list)
        embed.add_field(name = f'Assests Data: \n{assests_date_display}\n{assests_value_display}', value='\u200b', inline = False)

        # Total Revenues
        display_revenues_df = self.__quarterly_income_stmt_df.loc[['Total Revenue']] if not self.__quarterly_income_stmt_df.empty else self.__annual_income_stmt_df.loc[['Total Revenue']]
        revenues_date_list = display_revenues_df.columns.tolist()
        revenues_date_str_list = self.__convert_date_list_to_str(revenues_date_list)[:self.MAX_DISPLAY_RESULT]
        revenues_value_list = self.__convert_value_list(display_assests_df.values[0].tolist())[:self.MAX_DISPLAY_RESULT]

        check_revenues_len_list = revenues_date_str_list + revenues_value_list
        max_revenues_word_len = self.__get_max_str_len(check_revenues_len_list)
        max_revenues_word_len += self.NO_OF_WHITESPACE
        
        revenues_date_display = self.__get_concat_str(max_revenues_word_len, revenues_date_str_list)
        revenues_value_display = self.__get_concat_str(max_revenues_word_len, revenues_value_list)
        embed.add_field(name = f'Revenues Data: \n{revenues_date_display}\n{revenues_value_display}', value='\u200b', inline = False)
        
        # Total Expenses
        display_expenses_df = self.__quarterly_income_stmt_df.loc[['Total Expenses']] if not self.__quarterly_income_stmt_df.empty else self.__annual_income_stmt_df.loc[['Total Expenses']]
        expenses_date_list = display_expenses_df.columns.tolist()
        expenses_date_str_list = self.__convert_date_list_to_str(expenses_date_list)[:self.MAX_DISPLAY_RESULT]
        expenses_value_list = self.__convert_value_list(display_assests_df.values[0].tolist())[:self.MAX_DISPLAY_RESULT]

        check_expenses_len_list = expenses_date_str_list + expenses_value_list
        max_expenses_word_len = self.__get_max_str_len(check_expenses_len_list)
        max_expenses_word_len += self.NO_OF_WHITESPACE
        
        expenses_date_display = self.__get_concat_str(max_expenses_word_len, expenses_date_str_list)
        expenses_value_display = self.__get_concat_str(max_expenses_word_len, expenses_value_list)
        embed.add_field(name = f'Expenses Data: \n{expenses_date_display}\n{expenses_value_display}', value='\u200b', inline = False)
        
        #Institution Holders
        # if not self.__major_holders_df.empty():
        #     marjor_holder_field_list = self.__major_holders_df.index().tolist()
        #     percentage_value_list = self.__major_holders_df.values.flatten().tolist()
            
        #     check_marjor_holder_len_list = marjor_holder_field_list + percentage_value_list
        #     max_marjor_holder_word_len = self.__get_max_str_len(check_marjor_holder_len_list)
        #     max_marjor_holder_word_len += self.NO_OF_WHITESPACE
            
        #     marjor_holder_field_display = self.__get_concat_str(max_marjor_holder_word_len, marjor_holder_field_list)
        #     percentage_value_display = self.__get_concat_str(max_marjor_holder_word_len, percentage_value_list)
        #     embed.add_field(name = f'Major Holder Data: \n{marjor_holder_field_display}\n{percentage_value_display}', value='\u200b', inline = False)
            
