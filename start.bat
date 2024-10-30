@echo off
start wt new-tab -p "Command Prompt"  -d . --title "SmallCapInitialPop"^
            cmd /k "C:\Users\John\Downloads\test-workspace\stock_scanner\dist\small_cap_initial_pop_scanner\small_cap_initial_pop_scanner.exe";^
        new-tab -p "Command Prompt" -d . --title "YesterdayTopGainer"^
             cmd /k "C:\Users\John\Downloads\test-workspace\stock_scanner\dist\yesterday_top_gainer_scanner\yesterday_top_gainer_scanner.exe"^
        new-tab -p "Command Prompt" -d . --title "SmallCapIntraDayBreakout"^
            cmd /k "C:\Users\John\Downloads\test-workspace\stock_scanner\dist\small_cap_intra_day_breakout_scanner\small_cap_intra_day_breakout_scanner.exe"
exit