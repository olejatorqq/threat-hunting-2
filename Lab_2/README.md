    library(arrow)

    ## Some features are not enabled in this build of Arrow. Run `arrow_info()` for more information.

    ## 
    ## Attaching package: 'arrow'

    ## The following object is masked from 'package:utils':
    ## 
    ##     timestamp

    library(dplyr)

    ## 
    ## Attaching package: 'dplyr'

    ## The following objects are masked from 'package:stats':
    ## 
    ##     filter, lag

    ## The following objects are masked from 'package:base':
    ## 
    ##     intersect, setdiff, setequal, union

    library(stringr)
    library(lubridate)

    ## 
    ## Attaching package: 'lubridate'

    ## The following object is masked from 'package:arrow':
    ## 
    ##     duration

    ## The following objects are masked from 'package:base':
    ## 
    ##     date, intersect, setdiff, union

    library(ggplot2)

### Импорт датасета

    dataset <- arrow::open_dataset("gowiththeflow_20190826.csv", format = "csv", schema = schema(timestamp=int64(),src=utf8(),dst=utf8(),port=uint32(),bytes=uint32()))

### Задание 2: Надите утечку данных 2

#### График распределения количества пакетов за каждый час

#### Построив график, видно, что основная активность приходилась на 15:00-24:00 =&gt; Это и является рабочим временеи

            dataset %>%
              select(timestamp, src, dst, bytes) %>%
              collect() %>% # Add this line to collect data into R
              mutate(outside_traffic = (str_detect(src,"^((12|13|14)\\.)")&!str_detect(dst,"^((12|13|14)\\.)")), hour = hour(as.POSIXlt(timestamp/1000, origin = "1970-01-01"))) %>%
              filter(outside_traffic == TRUE, hour >=0 & hour <= 15) %>%
              group_by(src) %>%
              summarise(total_bytes = sum(bytes)) %>%
              arrange(desc(total_bytes))  %>%
              head(10)

    ## # A tibble: 10 × 2
    ##    src          total_bytes
    ##    <chr>              <dbl>
    ##  1 13.37.84.125  4355695968
    ##  2 13.48.72.30    699869643
    ##  3 14.51.75.107   664232091
    ##  4 14.51.30.86    662053441
    ##  5 12.59.25.34    656414986
    ##  6 13.39.46.94    645382172
    ##  7 12.56.32.111   644762456
    ##  8 14.57.50.29    644628807
    ##  9 12.58.68.102   634977381
    ## 10 12.37.36.110   613188438

#### Код построения графика

\#####dataset %&gt;% \##### select(timestamp, src, dst, bytes) %&gt;%
\##### mutate(external\_traffic = (str\_detect(src, “^((12|13|14)\\.)”)
& !str\_detect(dst, “^((12|13|14)\\.)”)), hour =
hour(as\_datetime(timestamp/1000))) %&gt;% \#####
filter(external\_traffic == TRUE, hour &gt;= 0 & hour &lt;= 24) %&gt;%
\##### group\_by(hour) %&gt;% \##### summarise(packets = n()) %&gt;%
\##### collect() %&gt;% \##### ggplot(., aes(x = hour, y = packets)) +
\##### geom\_line(color = “red”, size = 1.5) + \##### geom\_point(color
= “black”, size = 3) + \##### labs(title = “Распределение отправленных
пакет каждый час”, x = “Час”, y = “Количество пакетов”)
