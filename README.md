# DbTransferTool
项目背景：
在开发/运维工作中，需要将阿里云SQLServer数据库中的数据迁移到阿里云MySQL数据库，使用从网上下载的SQLyog软件的迁移速度太慢（大约100万行/小时），只能自己开发软件用来加快迁移速度，提高工作效率。
实战效果：
DbTransferTool的迁移速度，使用下来大约是2160万行/小时，是SQLyog的20多倍，也满足我们的需求了。
【备注】
（1）阿里云RDS SQLServer数据库规格为：数据库类型:SQL Server 2008 R2、CPU:12核、数据库内存:12000MB、最大IOPS:6000、最大连接数:1200；
    阿里云RDS MySQL数据库规格为：数据库类型:MySQL 5.7、CPU:8 核、数据库内存:16384MB、最大IOPS:8000、最大连接数:4000。
（2）迁移的表数据，每行的大小约为1KB。
（3）DbTransferTool不但解决了SQLyog迁移速度慢的问题，也解决了SQLyog不能“断点续迁”的问题。
（4）DbTransferTool已在“SQLServer->MySQL / SQLServer->SQLServer / MySQL->SQLServer / MySQL->MySQL”四种场景下测试通过（阿里云RDS），迁移速度都在1600万行/小时~2600万行/小时之间，第一种场景下（SQLServer->MySQL），迁移速度最快，第三种场景下（MySQL->SQLServer），迁移速度最慢。
