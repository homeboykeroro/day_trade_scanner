@echo off
start wt new-tab -p "Command Prompt"  -d . --title "Flush Scanner"^
            cmd /k "C:\Users\John\Downloads\test-workspace\stock_scanner\dist\initial_flush_scanner\initial_flush_scanner.exe";^
        new-tab -p "Command Prompt" -d . --title "Pop Scanner"^
             cmd /k "C:\Users\John\Downloads\test-workspace\stock_scanner\dist\initial_pop_scanner\initial_pop_scanner.exe"
exit