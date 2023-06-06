### Подключение пакетов

        library(arrow)
        library(dplyr)
        library(stringr)
        library(lubridate)
        library(ggplot2)
        library(stringr)

### Импорт датасета

    dataset <- arrow::open_dataset("gowiththeflow_20190826.csv", format = "csv", schema = schema(timestamp=int64(),src=utf8(),dst=utf8(),port=uint32(),bytes=uint32()))

#### График отправленных байтов по портам

### Фильтрация и выбор данных по условиям

    filtered_table <- dataset %>%
      filter(src != "13.37.84.125", src != "12.55.77.96", str_detect(src, "^((12|13|14)\\.)") & !str_detect(dst, "^((12|13|14)\\.)")) %>%
      select(src, bytes, port) %>%
      collect()

### Сортировка таблицы по порту и группировка по источнику и порту

    sorted_table <- filtered_table[order(filtered_table$port, decreasing = TRUE), ] %>% 
                     group_by(src, port)

### Вычисление суммы и среднего значения байтов по порту и объединение данных

    total_bytes_table <- sorted_table %>% 
                  summarize(port_sum = sum(bytes)) %>% 
                  group_by(port)

    ## `summarise()` has grouped output by 'src'. You can override using the `.groups`
    ## argument.

    average_bytes_table <- total_bytes_table %>% 
                   summarize(port_mean = mean(port_sum))

    merged_data_table <- merge(sorted_table, average_bytes_table, by = "port")

### Выбор наименьшей разницы источника данных после фильтрации

    filtered_data_ip <- filter(merged_data_table, 
                      str_detect(src, "13.37.84.125", negate = TRUE) & str_detect(src, "12.55.77.96", negate = TRUE)
                    )
    filtered_data_ip$difference <- filtered_data_ip$bytes - filtered_data_ip$port_mean
    min_diff_index <- which.min(filtered_data_ip$difference)

    min_diff_row <- filtered_data_ip[min_diff_index,]
    min_diff_row %>% 
      head(1) %>%
      select(src)

    ##                   src
    ## 27962997 15.94.59.123
