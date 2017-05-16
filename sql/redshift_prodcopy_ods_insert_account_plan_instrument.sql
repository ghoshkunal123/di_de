truncate table prodcopy.ods.account_plan_instrument;
insert into prodcopy.ods.account_plan_instrument select * from prodcopy.stage.account_plan_instrument;
