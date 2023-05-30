## Подключение пакетов

    library(arrow)
    library(dplyr)
    library(stringr)

### Импорт датасета

    file <- arrow::read_csv_arrow("gowiththeflow_20190826.csv",schema = schema(timestamp=int64(),src=utf8(),dst=utf8(),port=int32(),bytes=int32()))

### Задание 1: Надите утечку данных из Вашей сети

    filter(file,str_detect(src,"^((12|13|14)\\.)"),
             str_detect(dst,"^((12|13|14)\\.)",negate=TRUE)) %>% 
      select(src,bytes) %>%
      group_by(src)%>% 
      summarise(bytes=sum(bytes))%>%
      slice_max(bytes)%>%
      select(src)

    ## # A tibble: 1 × 1
    ##   src         
    ##   <chr>       
    ## 1 13.37.84.125
