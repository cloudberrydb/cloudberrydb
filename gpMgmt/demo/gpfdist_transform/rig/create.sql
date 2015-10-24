drop table if exists rig_xml;

create table rig_xml (
    well_uid    text,  -- e.g. W-12
    well_name   text,  -- e.g. 6507/7-A-42
    rig_uid     text,  -- e.g. xr31
    rig_name    text,  -- e.g. Deep Drill #5
    rig_owner   text,  -- e.g. Deep Drilling Co.
    rig_contact text,  -- e.g. John Doe
    rig_email   text,  -- e.g. John.Doe@his-ISP.com
    doc         xml
) distributed by (rig_uid);
