CREATE OR REPLACE FUNCTION f1()
RETURNS integer AS
$BODY$
BEGIN

return 1;

exception when others then
begin
raise exception 'Error during transaction. Error code raised %. Description: %', sqlstate, sqlerrm;
end;

END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;
