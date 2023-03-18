	public void execute(IETLBaseProcessEngine engine, Document modelNodeDoc, ETLProcessNodeInstanceEntry insNode,Document indoc) throws Exception {
		engine.setCommitCount(0);
		List<Document> indocs=engine.getData(indoc);//要写的数据流
		int maxWriteNum=DocumentUtil.getInteger(modelNodeDoc,"maxWriteNum");//最大写入
		String metaDataConfigId=modelNodeDoc.getString("metaDataConfigId"); //元数据id
//		String symbol=DocumentUtil.getString(modelNodeDoc, "symbol"); //转义符号
		String createTableFlag=DocumentUtil.getString(modelNodeDoc, "createTableFlag","0"); //1表示需要自动创建表结构，0表示否
		boolean nodeTransactionFlag=DocumentUtil.getString(modelNodeDoc, "nodeTransactionFlag","1").equals("1");//1表示支持，2表示不支持
		String dbConnId="";
		String tableName="";
		if(StringUtils.isNotBlank(metaDataConfigId)) {
			P_ModelConfigEntry modelConfigEntry=modelConfigDao.getModelByModelId(metaDataConfigId);
			if(modelConfigEntry==null) {throw new Exception("元数据模型("+metaDataConfigId+")不存在!");}
			dbConnId=modelConfigEntry.getDbConnId();
			tableName=modelConfigEntry.getTableName();
		}else {
			dbConnId=modelNodeDoc.getString("dbConnId"); //数据源
			tableName=modelNodeDoc.getString("tableName"); //tableName数据库表名
		}
		String tableColumns=DocumentUtil.getString(modelNodeDoc, "tableColumns","[]"); //字段列配置
		List<Document> nodeColumnConfigDocs=JsonUtil.jsonArray2docs(tableColumns);//所有当前节点中字段列的配置数据
		String targetDeleteAll=DocumentUtil.getString(modelNodeDoc,"targetDeleteAll","0"); //1表示新启动本节点时清空目标表中的数据,2表示每次执行时均清空数据，0表示不清空
		String schemaUserId=modelNodeDoc.getString("schemaUserId"); //用户身份
		String nodeId=modelNodeDoc.getString("pNodeId");
		
		ETLProcessDebugLogUtil.log(engine,"调试","提示:"+nodeId+"准备传输符合条件的("+indocs.size()+")条数据到("+tableName+")表中!");
		Connection conn=ETLProcessNodeCommonUtil.getNodeConnectionByTransactionIdNodeId(engine, indoc, modelNodeDoc, dbConnId,engine.getTransactionId(),nodeId);
		String dbType = ETLRdbUtil.getDbType(conn);
		String symbol = ETLRdbUtil.getSymbol(dbType);
		modelNodeDoc.put("symbol", symbol);
		boolean closeConnection=DocumentUtil.getString(modelNodeDoc, "closeConnection","true").equals("true");//false表示不关闭，true表示关闭
		if(StringUtils.isNotBlank(schemaUserId)) {
			ETLRdbUtil.changeSchema(conn, schemaUserId); //切换身份
		}
		//看是否自动创建表结构
		if(createTableFlag.equals("1")) {
			String createResult=ETLRdbMetaDataUtil.createTable(engine,conn, schemaUserId,tableName, nodeColumnConfigDocs);//1表示创建成功，0表示表已经存在，其他表示创建表失败的原因
			if(!createResult.equals("1") && !createResult.equals("0")) {
				throw new Exception("数据库表("+tableName+")创建失败,流程退出执行!");
			}
		}
		
		//判断最大写入数量
		if(maxWriteNum>0 && maxWriteNum<indocs.size()) {
			indocs=indocs.subList(0, maxWriteNum); //最大写入数量
		}
		
		//得到数据库表中的字段配置
		HashMap<String, Document> tableColumnsConfig=ETLRdbMetaDataUtil.listColumnsMetaMapForWriteByTableName(conn, tableName,"");//自动从表中取表的字段配置信息
		if(nodeColumnConfigDocs.size()==0) {throw new Exception("错误:输出节点("+nodeId+")没有配置输出字段!");}
		
		//得到主键字段
		String keyIds="";
		String queryKeys = "";
		List<String> allUpdateNodeWriteTableColumnConfigs=new ArrayList<String>();//节点中配置所有需要更新输出的配置字段
		List<String> allInsertNodeWriteTableColumnConfigs=new ArrayList<String>();//节点中配置所有插入时更新输出的配置字段
		List<String> hadValueCanUpdateFields=new ArrayList<String>();//非空字符串才可以更新的字段列表
		List<String> notNullCanUpdateFields=new ArrayList<String>(); //非空时才可更新的字段
		for(Document columnConfigDoc:nodeColumnConfigDocs) {
			//得到主键字段
			String colId=columnConfigDoc.getString("colId");
			boolean primaryKey=columnConfigDoc.getBoolean("primaryKey", false); //是否是主键
			int fieldConflictMode=DocumentUtil.getInteger(columnConfigDoc,"conflictMode");//0表示同步更新,1插入时更新，2禁止更新,3有值时(非null)更新,4非空字符串才更新
			if(primaryKey) {
				if(StringUtils.isNotBlank(keyIds)) {keyIds+=",";}
				keyIds+=colId;
				queryKeys+=ETLRdbUtil.formatBySymbol(colId, symbol);
			}
			if(fieldConflictMode==0) {
				//说明是同步更字段insert和update时字段都需要加入
				allUpdateNodeWriteTableColumnConfigs.add(colId.toUpperCase());
				allInsertNodeWriteTableColumnConfigs.add(colId.toUpperCase());
			}else if(fieldConflictMode==1) {
				//说明只有插入时才进行更新
				allInsertNodeWriteTableColumnConfigs.add(colId.toUpperCase());
			}else if(fieldConflictMode==2) {
				//禁止更新,不加入到输出字段中
			}else if(fieldConflictMode==3) {
				//流中有字段时可更新
				allUpdateNodeWriteTableColumnConfigs.add(colId.toUpperCase());
				allInsertNodeWriteTableColumnConfigs.add(colId.toUpperCase());
				notNullCanUpdateFields.add(colId.toUpperCase()); //not null 可以更新的字段列表
			}else if(fieldConflictMode==4) {
				//字段值非空时可更新
				allUpdateNodeWriteTableColumnConfigs.add(colId.toUpperCase());
				allInsertNodeWriteTableColumnConfigs.add(colId.toUpperCase());
				hadValueCanUpdateFields.add(colId.toUpperCase()); //非空字符串才可以更新的字段列表
			}
		}
		
		//复制一个只读的集合出来,防止被修改
		List<String> readOnlyUpdateNodeWriteTableColumnConfigs=Collections.unmodifiableList(allUpdateNodeWriteTableColumnConfigs); //更新时可以输出的字段，如果不想更新在此列表中删除掉字段
		List<String> readOnlyInsertNodeWriteTableColumnConfigs=Collections.unmodifiableList(allInsertNodeWriteTableColumnConfigs); //插入时可以输出的字段,如果不想插入需在此列表中删除掉字段
		
		
		//看是否先清空目标表中的数据,清空后再进行CRUD的判断更新
		if((targetDeleteAll.equals("1") && StringUtils.isBlank(insNode.getEndTime())) || targetDeleteAll.equals("2")) {
			//1表法只有新进入的节点时才清空，endTime为空时表示节点是第一次新进入的
			ETLProcessDebugLogUtil.log(engine, "调试",nodeId+"清空目标表中的数据("+(targetDeleteAll.equals("1")?"第一次启动":"每次执行")+"):"+dbConnId+"."+tableName);
			ETLRdbUtil.deleteAll(conn, tableName,symbol);
			if(nodeTransactionFlag==false) {
				ETLRdbUtil.commit(conn); //不支持事务的情况下先提交
			}
		}
		

		//开始进行数据传输
		String writeType=modelNodeDoc.getString("writeType"); //数据写入模式1逐条写入,2表示批量写入,3合并后批量,4Gauss导入阶段表
		if(writeType.equals("1")){
			//1.逐条判断更新或插入记录
			if(StringUtils.isBlank(keyIds)) {throw new Exception("错误:节点("+nodeId+")没有设置主键退出数据更新!!!");}
			List<Document> deleteDocs=new ArrayList<Document>(); //要删除的数据
			List<Document> otherDocs=new ArrayList<Document>(); //要删除的数据
			for(Document doc :indocs){
				if("D".equals(doc.getString("P_TAG_IUD"))){
					deleteDocs.add(doc)	;
				}else if ("clickhouse".equals(dbType)){
					for (String key : doc.keySet()){
						if (doc.get(key) instanceof Boolean) {
							boolean value = (boolean) doc.get(key);
							if (value) {
								doc.put(key, 1);
							}
							if (!value) {
								doc.put(key, 0);
							}
						}
					}
					otherDocs.add(doc);
				}
				else{
					otherDocs.add(doc);
				}
			}
			ETLProcessDebugLogUtil.log(engine,"调试","插入更新的数据量:"+otherDocs.size()+",删除的数据量:"+deleteDocs.size());
			//使用当前节点中的字段配置进行数据格式化,不删除没有配置的字段的值,因为没有配置的字段本身就不输出给目标表了
			ETLProcessNodeCommonUtil.formartTableColumnsDataByColumnsConfig(engine, modelNodeDoc, nodeColumnConfigDocs, nodeId, indocs,false);
			ETLJdbcDataWriteParamsEntry jdbcDataWriteParamsEntry=new ETLJdbcDataWriteParamsEntry();
			jdbcDataWriteParamsEntry.setConn(conn);
			jdbcDataWriteParamsEntry.setTableName(tableName);
			jdbcDataWriteParamsEntry.setEngine(engine);
			jdbcDataWriteParamsEntry.setModelNodeDoc(modelNodeDoc);
			jdbcDataWriteParamsEntry.setInsNode(insNode);
			jdbcDataWriteParamsEntry.setIndoc(indoc);
			jdbcDataWriteParamsEntry.setDataDocs(otherDocs);
			jdbcDataWriteParamsEntry.setKeyIds(keyIds);
			jdbcDataWriteParamsEntry.setReadOnlyInsertNodeWriteTableColumnConfigs(readOnlyInsertNodeWriteTableColumnConfigs);
			jdbcDataWriteParamsEntry.setReadOnlyUpdateNodeWriteTableColumnConfigs(readOnlyUpdateNodeWriteTableColumnConfigs);
			jdbcDataWriteParamsEntry.setHadValueCanUpdateFields(hadValueCanUpdateFields);
			jdbcDataWriteParamsEntry.setNotNullCanUpdateFields(notNullCanUpdateFields);
			jdbcDataWriteParamsEntry.setTableColumnsConfig(tableColumnsConfig);
			jdbcDataWriteParamsEntry.setNodeColumnConfigDocs(nodeColumnConfigDocs);
			jdbcDataWriteParamsEntry.setQueryKeys(queryKeys);
			//开始删除数据,不支持批量删除
			if(deleteDocs.size()>0) {
				ETLJdbcDataWriteUtil.deleteDataDocs(conn,tableName,engine,modelNodeDoc,insNode,deleteDocs,keyIds,indoc,tableColumnsConfig,symbol);
				//如果流程不支持事务则立即提交数据
				if(nodeTransactionFlag==false) {
					ETLRdbUtil.commit(conn);
				}
			}
			ETLJdbcDataWriteUtil.insertOrUpdateDocs(jdbcDataWriteParamsEntry); //进行逐条更新和插入
			//如果流程不支持事务则立即提交数据
			if(nodeTransactionFlag==false) {ETLRdbUtil.commit(conn);}
		}else if(writeType.equals("2")){
			//2.批量插入,批量插入不需要指定数据库输出表的主键
			//使用当前节点中的字段配置进行数据格式化,不删除没有配置的字段的值,因为没有配置的字段本身就不输出给目标表了
			ETLProcessNodeCommonUtil.formartTableColumnsDataByColumnsConfig(engine, modelNodeDoc, nodeColumnConfigDocs, nodeId, indocs,false); //批量插入前对null值进行处理和运算一次
			ETLJdbcDataWriteUtil.batchInsert(conn,tableName,engine,modelNodeDoc,insNode,indocs,indoc,readOnlyInsertNodeWriteTableColumnConfigs,tableColumnsConfig); //2020-10-13 symbol修改ok
			//如果流程不支持事务则立即提交数据
			if(nodeTransactionFlag==false) {
				ETLRdbUtil.commit(conn);
			}
		}else if(writeType.equals("3")){
			if(StringUtils.isBlank(keyIds)) {throw new Exception("错误:节点("+nodeId+")没有设置主键退出数据更新!!!");}
			//3合并后批量更新,先进行批量检测数据是否存在然后对数据进行crud的合并
			List<Document> insertDocs=new ArrayList<Document>(); //要插入的数据
			List<Document> updateDocs=new ArrayList<Document>(); //要更新的数据
			List<Document> deleteDocs=new ArrayList<Document>(); //要删除的数据
			//检测数据记录是CRUD状态
			ETLJdbcDataWriteUtil.checkDocIsExists(engine, conn, tableName, tableColumnsConfig, modelNodeDoc, indocs, insertDocs, updateDocs, deleteDocs, keyIds, symbol);
			ETLProcessDebugLogUtil.log(engine,"调试",nodeId+"数据合并结果:插入数据量:"+insertDocs.size()+",更新的数据量:"+updateDocs.size()+",删除的数据量:"+deleteDocs.size());
			//对所有数据进行一次字段事件的转换
			List<Document> allDocs=new ArrayList<Document>();
			allDocs.addAll(insertDocs);
			allDocs.addAll(deleteDocs);
			allDocs.addAll(updateDocs);
			//使用当前节点中的字段配置进行数据格式化,不删除没有配置的字段的值,因为没有配置的字段本身就不输出给目标表了
			ETLProcessNodeCommonUtil.formartTableColumnsDataByColumnsConfig(engine, modelNodeDoc, nodeColumnConfigDocs, nodeId, allDocs,false);
			//开始插入数据
			if(insertDocs.size()>0) {
				ETLJdbcDataWriteUtil.batchInsert(conn,tableName,engine,modelNodeDoc,insNode,insertDocs,indoc,readOnlyInsertNodeWriteTableColumnConfigs,tableColumnsConfig); //批量插入
				//如果流程不支持事务则立即提交数据
				if(nodeTransactionFlag==false) {
					ETLRdbUtil.commit(conn);
				}
			}
			
			//开始更新数据
			if(updateDocs.size()>0) {
				//合并后批量更新,2021-10-12 symbol转义符添加，同时修改bug由indocs改为updateDocs
				ETLJdbcDataWriteUtil.batchUpdate(conn,tableName, engine, modelNodeDoc, insNode, updateDocs, keyIds,indoc,readOnlyUpdateNodeWriteTableColumnConfigs,tableColumnsConfig);
				//如果流程不支持事务则立即提交数据
				if(nodeTransactionFlag==false) {
					ETLRdbUtil.commit(conn);
				}
			}
			
			//开始删除数据,不支持批量删除
			if(deleteDocs.size()>0) {
				ETLJdbcDataWriteUtil.deleteDataDocs(conn,tableName,engine,modelNodeDoc,insNode,deleteDocs,keyIds,indoc,tableColumnsConfig,symbol);
				//如果流程不支持事务则立即提交数据
				if(nodeTransactionFlag==false) {
					ETLRdbUtil.commit(conn);
				}
			}
		}else if(writeType.equals("4")){
			//导入阶段表(更新、插入)操作,只有Gauss数据库类型才有
			if(StringUtils.isBlank(keyIds)) {throw new Exception("错误:节点("+nodeId+")没有设置主键退出数据更新!!!");}
			//避免表存在导致创建失败(原表名_时间yyyyMMddHHmmss_temp)
			String tempTableName = tableName + "_" + DateTimeUtil.getNow("yyyyMMddHHmmss") + "_temp";
			//是否创建成功临时表
			boolean hasCreateTempTable = false;

			//1.开始根据原表创建临时表
			try {
				//校验需要创建的临时表是否存在。存在则不进行后续操作
				boolean hasTableFlag = ETLRdbMetaDataUtil.HasTable(conn, tempTableName, null);
				if (!hasTableFlag) {
					//1.临时表不存在，进行创建临时表
					String createTempTableSQL = "create temp table " + tempTableName + " (like " + tableName + " INCLUDING all )";
					ETLProcessDebugLogUtil.log(engine, "调试", "准备执行创建临时表SQL: " + createTempTableSQL);
					ETLProcessDebugLogUtil.log(engine, "调试", "executeUpdate");
					ETLRdbUtil.executeUpdateSql(conn, createTempTableSQL);
					hasCreateTempTable = true;
					ETLProcessDebugLogUtil.log(engine,"调试","提示:临时表("+tempTableName+")创建成功");

					//2.创建并执行insert语句
					//开始批量插入数据到临时表
					ETLProcessNodeCommonUtil.formartTableColumnsDataByColumnsConfig(engine, modelNodeDoc, nodeColumnConfigDocs, nodeId, indocs,false); //批量插入前对null值进行处理和运算一次
					ETLProcessDebugLogUtil.log(engine,"调试","提示:开始批量插入"+indocs.size()+"条数据到"+tempTableName+"数据库表中");
					ETLRdbUtil.batchInsert(engine,conn, tempTableName, indocs, 0,readOnlyInsertNodeWriteTableColumnConfigs,tableColumnsConfig,symbol,true,"");
					ETLProcessDebugLogUtil.log(engine,"调试","提示:数据插入临时表("+tempTableName+")成功");

					List<String> keyList = CommonUtil.splitAsList(keyIds, ",");

					StringBuilder privateKeySB = new StringBuilder();
					for (int i = 0; i < keyList.size(); i++) {
						privateKeySB.append("a.").append(keyList.get(i)).append("=").append("b.").append(keyList.get(i));
						if (keyList.size()-i!=1) {
							privateKeySB.append(" and ");
						}
					}

					StringBuilder updateValueSB = new StringBuilder();
					for (String key : keyList) {
						for (int i = 0; i < allUpdateNodeWriteTableColumnConfigs.size(); i++) {
							if (!allUpdateNodeWriteTableColumnConfigs.get(i).toLowerCase().equals(key)) { //排除更新主键字段，否则报错
								updateValueSB.append("a.").append(allUpdateNodeWriteTableColumnConfigs.get(i)).append("=").append("b.").append(allUpdateNodeWriteTableColumnConfigs.get(i));
								if (allUpdateNodeWriteTableColumnConfigs.size() - i != 1) {
									updateValueSB.append(" and ");
								}
							}
						}
					}

					StringBuilder insertValueSB = new StringBuilder();
					for (int i = 0; i < allInsertNodeWriteTableColumnConfigs.size(); i++) {
						insertValueSB.append("b.").append(allInsertNodeWriteTableColumnConfigs.get(i));
						if (allInsertNodeWriteTableColumnConfigs.size()-i!=1) {
							insertValueSB.append(",");
						}
					}

					//3.merge合并临时表数据到原表
					StringBuilder mergeSQLSB = new StringBuilder();
					mergeSQLSB.append("MERGE INTO ").append(tableName).append(" ").append("a ")
							.append("USING ").append(tempTableName).append(" ").append("b ")
							.append("ON ").append("(").append(privateKeySB).append(") ")
							.append("WHEN MATCHED THEN ")
							.append("UPDATE SET ").append(updateValueSB)
							.append(" WHEN NOT MATCHED THEN ")
							.append("INSERT VALUES").append("(").append(insertValueSB).append(")")
							.append(";");

					ETLProcessDebugLogUtil.log(engine, "调试", "准备执行合并MergeSQL: " + mergeSQLSB);
					ETLRdbUtil.executeUpdateSql(conn, mergeSQLSB.toString());
					ETLProcessDebugLogUtil.log(engine,"调试","提示:合并MergeSQL执行成功");

					//记录为更新数量
					insNode.setUpdateSuccessCount(insNode.getUpdateSuccessCount() + indocs.size());
					insNode.setLastUpdateSuccessCount(indocs.size());
					insNode.addWriteCountDetails(indocs.size());

				}else {
					ETLProcessDebugLogUtil.log(engine,"调试","临时表("+tempTableName+")已存在，跳过插入、更新操作");
				}
			}catch (Throwable t){
				//记录失败更新数
				insNode.setInsertFailedCount(insNode.getInsertFailedCount()+engine.getDataTotalCount(indoc)); //记录为插入失败的数据
				insNode.setLastInsertFailedCount(insNode.getInsertFailedCount());

				String exceptionMsg=t.getMessage();
				ExceptionUtil.log(t);
				ETLProcessDebugLogUtil.log(engine, "异常", "创建临时表异常:"+exceptionMsg);
				throw t;
			}finally {
				//4.删除临时表
				if (hasCreateTempTable) {
					String dropSQL = "DROP TABLE "+tempTableName;
					ETLRdbUtil.executeUpdateSql(conn, dropSQL);
					ETLProcessDebugLogUtil.log(engine,"调试","提示:临时表("+tempTableName+")删除成功");
				}
			}
			//如果流程不支持事务则立即提交数据
			if(nodeTransactionFlag==false) {
				ETLRdbUtil.commit(conn);
			}
		}else{
			ETLProcessDebugLogUtil.log(engine,"调试","不支持的数据写入方式，请确认数据模型节点("+insNode.getpNodeId()+")配置是否正确");
		}
		//关闭链接2021.8.22
		if(closeConnection) {
			//ETLProcessDebugLogUtil.log(engine,"调试",nodeId+"准备关闭数据库链接="+conn);
			ETLRdbUtil.closeConn(conn);
			//ETLProcessDebugLogUtil.log(engine,"调试",nodeId+"数据库链接关闭结果="+conn.isClosed());
		}
		
	}
