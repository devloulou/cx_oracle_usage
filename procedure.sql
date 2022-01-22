create or replace procedure get_day_name_pcd(p_date in date, p_retval out varchar2) is
begin
    select trim(to_char(p_date, 'day')) into p_retval from dual ;
end;
