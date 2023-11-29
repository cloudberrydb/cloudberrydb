
-- load data for hits and hits_tmp tables

\copy hits_tmp from 'data/hits.data';

SET vector.enable_vectorization=OFF;

insert into hits select
watchid::numeric/2, javaenable, title, goodevent, eventtime, eventdate, counterid, clientip, clientip6, regionid,
userid::numeric/2, counterclass, os, useragent, url, referer, urldomain, refererdomain, refresh, isrobot, referercategories, urlcategories,
urlregions, refererregions, resolutionwidth, resolutionheight, resolutiondepth, flashmajor, flashminor, flashminor2, netmajor, netminor,
useragentmajor, useragentminor, cookieenable, javascriptenable, ismobile, mobilephone, mobilephonemodel, params, ipnetworkid, traficsourceid,
searchengineid, searchphrase, advengineid, isartifical, windowclientwidth, windowclientheight, clienttimezone, clienteventtime,
silverlightversion1, silverlightversion2, silverlightversion3, silverlightversion4, pagecharset, codeversion, islink, isdownload,
isnotbounce, funiqid::numeric/2, hid, isoldcounter, isevent, isparameter, dontcounthits, withhash, hitcolor, utceventtime, age, sex,
income, interests, robotness, generalinterests, remoteip, remoteip6, windowname, openername, historylength, browserlanguage, browsercountry,
socialnetwork, socialaction, httperror, sendtiming, dnstiming, connecttiming, responsestarttiming, responseendtiming, fetchtiming,
redirecttiming, dominteractivetiming, domcontentloadedtiming, domcompletetiming, loadeventstarttiming, loadeventendtiming,
nstodomcontentloadedtiming, firstpainttiming, redirectcount, socialsourcenetworkid, socialsourcepage, paramprice, paramorderid,
paramcurrency, paramcurrencyid, goalsreached, openstatservicename, openstatcampaignid, openstatadid, openstatsourceid, utmsource, utmmedium,
utmcampaign, utmcontent, utmterm, fromtag, hasgclid, refererhash::numeric/2, urlhash::numeric/2, clid, yclid::numeric/2, shareservice,
shareurl, sharetitle, key1, key2, key3, key4, key5, valuedouble, islandid, requestnum, requesttry
from hits_tmp;

SET vector.enable_vectorization=ON;
