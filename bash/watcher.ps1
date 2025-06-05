$file = "PathFile$"

while ($true) {
    try {
        $excel = New-Object -ComObject Excel.Application
        $excel.Visible = $false
        $excel.DisplayAlerts = $false
        
        $workbook = $excel.Workbooks.Open($file)
        $workbook.RefreshAll()
        
        # Ждем завершения обновления
        Start-Sleep -Seconds 100
        
        $workbook.Save()
        $workbook.Close()
    }
    catch {
        Write-Host "Ошибка: $_"
    }
    finally {
        if ($workbook) {
            [System.Runtime.Interopservices.Marshal]::ReleaseComObject($workbook) | Out-Null
        }
        if ($excel) {
            $excel.Quit()
            [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
        }
        [System.GC]::Collect()
        [System.GC]::WaitForPendingFinalizers()
    }
    
    # Ожидание перед следующим открытием файла
    Start-Sleep -Seconds 700
}
