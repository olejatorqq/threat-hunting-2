### Подключение пакетов

    library(arrow)
    library(dplyr)
    library(stringr)
    library(lubridate)
    library(ggplot2)

### Импорт датасета

    dataset <- arrow::open_dataset("gowiththeflow_20190826.csv", format = "csv", schema = schema(timestamp=int64(),src=utf8(),dst=utf8(),port=uint32(),bytes=uint32()))

#### График отправленных байтов по портам

### 1. Нахождение портов с наименее отправленными на них данными

    dataset %>%
      select(src, dst, bytes,port) %>%
      mutate(outside_traffic = (str_detect(src,"^((12|13|14)\\.)") & !str_detect(dst,"^((12|13|14)\\.)"))) %>%
      filter(outside_traffic == TRUE) %>%
      group_by(port) %>%
      summarise(total_data=sum(bytes)) %>%
      filter(total_data < 5*10^9) %>%
      select(port) %>%
      collect() -> ports

    ports <- unlist(ports)
    ports <- as.vector(ports,'numeric')

### 2. Выборка данных с нужными номерами портов

    dataset %>%
      select(src, dst, bytes,port) %>%
      mutate(outside_traffic = (str_detect(src,"^((12|13|14)\\.)") & !str_detect(dst,"^((12|13|14)\\.)"))) %>%
      filter(outside_traffic == TRUE) %>%
      filter(port %in% ports) %>%
      group_by(src,port) %>%
      summarise(total_bytes=sum(bytes)) %>%
      arrange(desc(port)) %>%
      collect() -> dataframe

### 3. Список портов с максимальным количеством данных

    dataframe %>%
      group_by(src, port) %>%
      summarise(total_data=sum(total_bytes)) %>%
      arrange(desc(total_data)) %>%
      head(10) %>%
      collect()

    ## # A tibble: 10 × 3
    ## # Groups:   src [3]
    ##    src           port total_data
    ##    <chr>        <int>      <int>
    ##  1 13.37.84.125    36 2070876332
    ##  2 13.37.84.125    95 2031985904
    ##  3 13.37.84.125    21 2027501066
    ##  4 13.37.84.125    78 2018366254
    ##  5 13.37.84.125    32 1989408807
    ##  6 12.55.77.96     31  233345180
    ##  7 13.48.72.30     26    2468348
    ##  8 13.48.72.30     61    2465805
    ##  9 13.48.72.30     77    2453566
    ## 10 13.48.72.30     79    2421971

### 4. Список количества хостов к портам

    dataframe %>%
      group_by(port) %>%
      summarise(hosts=n()) %>%
      arrange(hosts) %>%
      head(10) %>%
      collect()

    ## # A tibble: 10 × 2
    ##     port hosts
    ##    <int> <int>
    ##  1    21     1
    ##  2    31     1
    ##  3    32     1
    ##  4    36     1
    ##  5    78     1
    ##  6    95     1
    ##  7    51    24
    ##  8    22  1000
    ##  9    23  1000
    ## 10    25  1000

### Список количества хостов к портам

#### На основании предыдущих шагов можно сделать вывод, что злоумышленник использует IP-адрес 12.55.77.96 и порт 31. Это можно сказать, исходя из результатов 4 шага, где видно, что только один хост использовал порт 31, а также из результатов 3 шага, где видно, что наибольшее количество данных было передано именно по этому порту.

    dataframe %>%
      filter(port == 31) %>%
      group_by(src) %>%
      summarise(total_data=sum(total_bytes)) %>%
      collect()

    ## # A tibble: 1 × 2
    ##   src         total_data
    ##   <chr>            <int>
    ## 1 12.55.77.96  233345180

#### График отправленных байтов по портам

    dataset %>%
      select(timestamp, src, dst, bytes, port) %>%
      mutate(external_traffic = (str_detect(src, "^((12|13|14)\\.)") & !str_detect(dst, "^((12|13|14)\\.)")), 
             hour = hour(as_datetime(timestamp/1000))) %>%
      filter(external_traffic == TRUE, hour >= 0 & hour <= 24) %>%
      group_by(port) %>%
      summarise(bytes = sum(bytes)) %>%
      collect() %>%
      ggplot(., aes(x = port, y = bytes)) +
      geom_bar(stat = "identity", fill = "blue") +
      labs(title = "Распределение отправленных байтов по портам", x = "Порт", y = "Количество байтов")
