drop table if exists mef_xml;

create table mef_xml (
    id         text, 
    doc        xml
) distributed by (id);
