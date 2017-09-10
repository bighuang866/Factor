classdef Factor < handle
    %FACOTR 用于操作更新因子数据库的类
    %   LAST UPDATE 2017/7/13 BY BIG HUANG
    %   FACOTR properties:
    %   public:
    %         DBPath                                - 数据库文件所在路径
    %         factorFunPath                         - 因子函数所在路径
    %   private:
    %         factorDBConn                          - 与DBPath文件夹下数据库的连接,map格式
    %                                                 键为数据库名称字符串,值为与对应数据的连接
    %         factorDBName                          - DBPath文件夹下数据库的名称字符串,cell格式
    %         factorFunHandle                       - factorFunPath文件夹下的因子函数的句柄,map格式
    %                                                 键为数据库名称字符串,值为一个元胞,储存有该数据库下所有因子的函数句柄
    %         factorFunNames                        - factorFunPath文件夹下的因子函数的名称,map格式
    %                                                 键为数据库名称字符串,值为一个元胞,储存有该数据库下所有因子的函数名称
    %         windDBConn                            - 与wind数据库的连接
    %
    %   FACOTR methods and functions:
    %   public:
    %     创建类:
    %         Factor                                - 构造函数,初始化Factor对象
    %     建表及更新操作：
    %         createAllTable                        - 创建数据库所有的因子
    %         updateAllTable                        - 更新数据库所有的因子
    %         createSingleTable                     - 创建数据库里的单个因子
    %         updateSingleTable                     - 更新数据库里的单个因子
    %         oneClickUpdate                        - 一键更新数据库所有因子,创建所有未创建的因子
    %         createAllIndex                        - 创建数据库所有的因子的索引
    %         dropAllIndex                          - 删除数据库所有的因子的索引
    %     查询表的状态：
    %         qryDBInfo                             - 查询数据库中已存在的表的信息
    %         qryDBTables                           - 查询数据库中当前有哪些表
    %     其他：
    %         closeAllConn                          - 关闭所有与数据库的连接
    %   private:
    %     初始化设置类属性：
    %         setFactorDBConn                       - 设置属性factorDBConn
    %         setFactorDBName                       - 设置属性factorDBName
    %         setFactorFunHandleAndFactorFunNames   - 设置属性factorFunHandle和FactorFunNames
    %         setWindDBConn                         - 设置属性windDBConn
    %         addFactorFunPath                      - 把因子函数的路径添加到搜索路径
    %     其他：
    %         checkFactor                           - 检查factorFunPath下的因子是否都已经在数据库中建立了表
    %         commitDB                              - 提交数据库事务
    %         rollbackDB                            - 回滚数据库操作
    %         findFactorBelongToDB                  - 找到指定因子是属于哪个数据库,
    %         private_createSingleTable             - 创建数据库里的单个因子,私有方法
    %         private_updateSingleTable             - 更新数据库里的单个因子,私有方法
    %         private_createSingleIndex             - 创建数据库里的单个因子的索引,私有方法
    %         private_dropSingleIndex               - 删除数据库里的单个因子的索引,私有方法
    %         qryLastUpdate                         - 查询某个因子的最后更新日期
    %         cleanDB                               - 清理数据库
    
    %% 公有属性
    properties(Access = 'public')
        % 数据库文件所在路径
        DBPath
        % 因子函数所在路径
        factorFunPath
    end
    
    %% 私有属性
    properties(Access = 'private', Hidden = true)
        % 与DBPath文件夹下数据库的连接,map格式
        % 键为数据库名称字符串,值为与对应数据的连接
        factorDBConn
        % DBPath文件夹下数据库的名称字符串,cell格式
        factorDBName
        % factorFunPath文件夹下的因子函数的句柄,map格式
        % 键为数据库名称字符串,值为一个元胞,储存有该数据库下所有因子的函数句柄
        factorFunHandle
        % factorFunPath文件夹下的因子函数的名称,map格式
        % 键为数据库名称字符串,值为一个元胞,储存有该数据库下所有因子的函数名称
        factorFunNames
        % 与wind数据库的连接
        windDBConn
    end
    
    %% 公有方法
    methods(Access = 'public')
        function obj = Factor(DBPath, factorFunPath, linkOrNot)
            %FACTOR 构造函数,初始化Factor对象
            %   输入参数
            %   DBPath          数据库的路径
            %   factorFunPath   因子函数的文件夹路径
            %
            %   可选输入
            %   linkOrNot       是否连接wind本地库,默认为是,若不连接则无法对数据库进行更新
            %
            %   返回一个Factor对象,用户可以使用Factor对象的一系列方法对数据库进行更新操作
            %%
            if nargin == 2
                linkOrNot = true;
            end
            % 设置属性DBPath
            obj.DBPath = DBPath;
            % 设置属性factorFunPath
            obj.factorFunPath = factorFunPath;
            % 设置属性factorDBName
            obj.setFactorDBName()
            % 设置属性factorDBConn
            obj.setFactorDBConn()
            % 设置属性factorFunHandle
            obj.setFactorFunHandleAndFactorFunNames()
            % 设置属性windDBConn
            obj.setWindDBConn(linkOrNot);
            % 把因子函数的路径添加到搜索路径
            obj.addFactorFunPath()
            % 回滚数据库操作
            obj.rollbackDB()
        end  
        function oneClickUpdate(obj, startDate, endDate)
            %ONECLICKUPDATE 一键更新数据库所有因子,创建所有未创建的因子
            %    输入参数
            %    startDate    开始日期
            %    endDate      结束日期
            %
            %    一键更新数据库所有因子,创建所有未创建的因子
            %    如果因子函数中的因子多于已经在数据库中建表的因子
            %    比如新写了一个因子的函数,放入factorFunPath路径下的文件夹里
            %    该函数在更新时会自动检查未在数据库中建表的因子
            %    先更新数据库中已有的表,再将新的因子在数据库中建表,并更新
            
            %%
            % 回滚数据库操作
            obj.rollbackDB()
            fprintf('开始检查数据库中已有的表\n');
            dbTables = obj.qryDBTables();
            
            tmpKeys = dbTables.keys;
            % 更新已有的表
            for i = 1:length(dbTables)
                tmpFactors = dbTables(tmpKeys{i});
                if ~isempty(dbTables(tmpKeys{i}))
                    for j = 1:size(tmpFactors, 1)
                        obj.private_updateSingleTable(endDate, tmpFactors{j, 1});
                        obj.commitDB();
                    end
                end
            end
            
            % 检查数据库中有哪些表没创建
            [flag, info] = obj.checkFactor();
            % 创建并更新新的表
            if ~flag
                fprintf('开始创建数据库中未创建的表\n');
                fprintf('新加入如下因子\n')
                for i = 1:size(info, 1)
                    fprintf('数据库%s 中的因子%s\n', info{i, 1}, info{i, 2});
                end
                for i = 1:size(info, 1)
                    obj.private_createSingleTable(startDate, endDate, info{i, 2})
                    obj.commitDB();
                end
            end
            % 提交确认数据库更改
            obj.cleanDB()
            obj.commitDB();
        end
        function createAllTable(obj, startDate, endDate)
            %CREATEALLTABLE 创建数据库所有的因子
            %    输入参数
            %    startDate    开始日期
            %    endDate      结束日期
            %
            %    创建数据库里所有的因子
            %    创建表需要开始日期及结束日期
            %    仅适用于数据库中的因子都没有建表的情况
            %    如果数据库中已经有建好的表,该函数会报错,错误为table already exists
            
            %%
            obj.rollbackDB()
            fprintf('开始创建数据库中所有的表\n');
            for i = 1:length(obj.factorDBName)
                factorNames = obj.factorFunNames(obj.factorDBName{i});
                for j = 1:length(factorNames)
                    obj.private_createSingleTable(startDate, endDate, factorNames{j});
                end
            end
            obj.commitDB();
        end
        function updateAllTable(obj, endDate)
            %UPDATEALLTABLE 更新数据库所有的因子
            %    输入参数
            %    endDate      结束日期
            %
            %    更新数据库里所有的因子
            %    更新表只需要结束日期,开始日期会自动生成
            %    仅会更新数据库中所有已创建的表,如果该表未创建,不会先创建在更新该表
            
            %%
            obj.rollbackDB()
            fprintf('开始更新数据库中所有的表\n');
            for i = 1:length(obj.factorDBName)
                factorNames = obj.factorFunNames(obj.factorDBName{i});
                for j = 1:length(factorNames)
                    obj.private_updateSingleTable(endDate, factorNames{j});
                end
            end
            obj.commitDB();
        end
        function createAllIndex(obj)
            %CREATEALLINDEX 创建数据库所有因子的索引
            %    输入参数
            %    无
            %
            %    创建数据库所有因子的索引
            %    仅适用于数据库中的因子都没有建立索引的情况
            %    如果数据库中的表已经建好索引,该函数会报错,错误为index already exists
            
            %%
            obj.rollbackDB()
            fprintf('开始创建数据库中所有的表的索引\n');
            for i = 1:length(obj.factorDBName)
                factorNames = obj.factorFunNames(obj.factorDBName{i});
                for j = 1:length(factorNames)
                    obj.private_createSingleIndex(factorNames{j})
                end
            end
            obj.commitDB();
        end
        function dropAllIndex(obj)
            %DROPALLINDEX 创建数据库所有因子的索引
            %    输入参数
            %    无
            %
            %    删除数据库所有因子的索引
            
            %%
            obj.rollbackDB()
            fprintf('开始删除数据库中所有的索引\n');
            for i = 1:length(obj.factorDBName)
                factorNames = obj.factorFunNames(obj.factorDBName{i});
                for j = 1:length(factorNames)
                    obj.private_dropSingleIndex(factorNames{j})
                end
            end
            obj.commitDB();
        end
        function createSingleTable(obj, startDate, endDate, factorName)
            %CREATESINGLETABLE 创建数据库里的单个因子
            %    输入参数
            %    startDate    开始日期
            %    endDate      结束日期
            %    factorName   字符串格式的因子名,如'FACTOR_SP_TTM'
            %
            %    创建数据库里的单个因子
            %    创建表需要开始日期及结束日期
            %    仅适用于数据库中的该因子没有建表的情况
            %    如果数据库中已经有建好的表,该函数会报错,错误为table already exists
            
            %%
            obj.rollbackDB();
            obj.private_createSingleTable(startDate, endDate, factorName);
            obj.commitDB();
        end
        function updateSingleTable(obj, endDate, factorName)
            %UPDATESINGLETABLE 更新数据库单个因子
            %    输入参数
            %    endDate      结束日期
            %    factorName   字符串格式的因子名,如'FACTOR_SP_TTM'
            %
            %    更新数据库单个因子
            %    更新表只需要结束日期,开始日期会自动生成
            %    仅会更新数据库中所有已创建的表,如果该表未创建,不会先创建在更新该表
            
            %%
            obj.rollbackDB();
            obj.private_updateSingleTable(endDate, factorName);
            obj.commitDB();
        end
        function dbTables = qryDBTables(obj)
            %QRYDBTABLES 查询数据库中当前有哪些表
            %    输入参数
            %    无
            %
            %    查询数据库中当前有哪些表
            %    返回一个map格式的变量,键为数据库的名称,值为对应的数据库里有哪些表
            
            %%
            dbTables = containers.Map();
            for i = 1:length(obj.factorDBName)
                try
                    tmp = tables(obj.factorDBConn(obj.factorDBName{i}));
                    dbTables(obj.factorDBName{i}) = tmp(ismember(tmp(:, 2), 'TABLE'), 1);
                catch
                    dbTables(obj.factorDBName{i}) = {};
                end
            end
        end  
        function dbInfoTab = qryDBInfo(obj)
            %QRYDBINFO 查询数据库中已存在的表的信息
            %    输入参数
            %    无
            %
            %    查询数据库中已存在的表的信息
            %    返回一个table,每列分别为：
            %    表名称,所在的数据库名称,更新开始时间,更新结束时间
            
            %%
            dbInfo = cell(0, 4);
            for i = 1:length(obj.factorDBName)
                tmp = tables(obj.factorDBConn(obj.factorDBName{i}));
                tableNames = tmp(ismember(tmp(:, 2), 'TABLE'), 1);
                for j = 1:length(tableNames)
                    sqlstr = sprintf('PRAGMA table_info(%s)', tableNames{j});
                    tmpTableInfo = DBFetch(obj.factorDBConn(obj.factorDBName{i}), sqlstr, 'table');
                    if any(ismember(tmpTableInfo.name, 'tradedate'))
                        dateCol = 'tradedate';
                    elseif  any(ismember(tmpTableInfo.name, 'issuingdate'))
                        dateCol = 'issuingdate';
                    else
                        error('数据库表内没有tradedate或issuingdate的日期列');
                    end
                    sqlstr = sprintf(['SELECT MIN(%s) AS startDate, MAX(%s) AS endDate'...
                        ' FROM %s'], dateCol, dateCol, tableNames{j});
                    tmp1 = DBFetch(obj.factorDBConn(obj.factorDBName{i}), sqlstr);
                    dbInfo(end+1, :) = {tableNames{j}, obj.factorDBName{i}, tmp1{1}, tmp1{2}};%#ok
                end
            end
            dbInfoTab = cell2table(dbInfo, 'VariableNames', {'tablename', 'databasename', 'startDate', 'endDate'});
        end
        function closeAllConn(obj)
            %CLOSEALLCONN 关闭所有与数据库的连接
            %    输入参数
            %    无
            %
            %    关闭所有与数据库的连接
            %    包括与因子数据库的连接和与wind数据库的连接
            
            %%
            
            % 关闭与wind数据库的连接
            close(obj.windDBConn);
            for i = 1:length(obj.factorDBConn)
                % 关掉对应因子数据库的连接
                close(obj.factorDBConn(obj.factorDBName{i}));
            end
        end
    end
    
    %% 私有方法
    methods(Access = 'private', Hidden = true)
        function setFactorDBName(obj)
            %SETFACTORDBNAME 设置属性factorDBName
            %    输入参数
            %    无
            %
            %    设置属性factorDBName
            %    设置数据库名称
            %    数据库的名称即为factorFunPath路径下的各个文件夹名称
            
            %%
            % 读取factorFunPath路径下的各个文件夹名称,就是数据库名称
            obj.factorDBName = readFolder(obj.factorFunPath, '');
        end
        % 设置属性factorDBConn
        function setFactorDBConn(obj)
            %SETFACTORDBCONN % 设置属性factorDBConn
            %    输入参数
            %    无
            %
            %    设置属性factorDBConn
            %    建立与因子数据库的连接
            %    数据库的名称即为factorFunPath路径下的各个文件夹名称
            
            %%
            obj.factorDBConn = containers.Map();
            for i = 1:length(obj.factorDBName)
                obj.factorDBConn(obj.factorDBName{i}) = DBlinkFACTOR(obj.DBPath, [obj.factorDBName{i}, '.db']);
            end
        end
        function setFactorFunHandleAndFactorFunNames(obj)
            %SETFACTORFUNHANDLEANDFACTORFUNNAMES % 设置属性factorFunHandle和FactorFunNames
            %    输入参数
            %    无
            %
            %    设置属性factorFunHandle和factorFunNames
            %    获取因子函数的名称和因子函数的函数句柄
            %    factorFunNames 即为factorFunPath路径下的各个文件夹中的m文件的名字
            %    factorFunHandle 即为factorFunPath路径下的各个文件夹中的m文件的名字转换成的函数句柄
            
            %%
            obj.factorFunHandle = containers.Map();
            obj.factorFunNames = containers.Map();
            for i = 1:length(obj.factorDBName)
                currentFolder = [obj.factorFunPath, '\', obj.factorDBName{i}];
                tmpMFiles = readFolder(currentFolder, 'm');
                f = @(str) regexprep(str, '\.m', '');
                funName = cellfun(f, tmpMFiles, 'UniformOutput', false);
                %                 f1 = @(str)  regexprep(str, 'FACTOR_', '');
                %                 obj.factorFunNames(obj.factorDBName{i}) = cellfun(f1, funName, 'UniformOutput', false);
                obj.factorFunNames(obj.factorDBName{i}) = funName;
                obj.factorFunHandle(obj.factorDBName{i}) = cellfun(@str2func, funName, 'UniformOutput', false);
            end
        end
        function setWindDBConn(obj,linkOrNot)
            %SETWINDDBCONN % 设置属性windDBConn
            %    输入参数
            %    linkOrNot       是否连接wind本地库,默认为是,若不连接则无法对数据库进行更新
            %
            %    设置属性windDBConn
            %    建立与wind数据库的连接
            
            %%
            if linkOrNot
                obj.windDBConn = DBlinkWind();
            else
                warning('未设置连接wind数据库,将无法进行更新数据库的操作')
                obj.windDBConn = '';
            end
            
        end
        function addFactorFunPath(obj)
            %ADDFACTORFUNPATH % 把因子函数的路径添加到搜索路径
            %    输入参数
            %    无
            %
            %    把因子函数的路径添加到搜索路径
            %    也就是factorFunPath
            %    这样之后调用因子函数的句柄时才可以运行
            
            %%
            for i = 1:length(obj.factorDBName)
                addpath([obj.factorFunPath,'\',obj.factorDBName{i}])
            end
        end
        function commitDB(obj)
            %COMMITDB 提交数据库事务
            %    输入参数
            %    无
            %
            %    提交数据库事务
            %    确认之前对数据库的所有更改
            %%
            for i = 1:length(obj.factorDBConn)
                commit(obj.factorDBConn(obj.factorDBName{i}));
            end
        end
        function rollbackDB(obj)
            %ROLLBACKDB 回滚数据库操作
            %    输入参数
            %    无
            %
            %    回滚数据库操作
            %    将未提交的数据库事务取消
            
            %%
            for i = 1:length(obj.factorDBConn)
                % 回滚对应数据库
                rollback(obj.factorDBConn(obj.factorDBName{i}));
            end
        end
        function dbName = findFactorBelongToDB(obj, factorName)
            %FINDFACTORBELONGTODB % 找到指定因子是属于哪个数据库,
            %    输入参数
            %    无
            %
            %    找到指定因子是属于哪个数据库
            %    也就是因子同名的函数在factorFunPath下的哪个文件夹
            
            %%
            for i = 1:length(obj.factorDBName)
                tmpFunNames = obj.factorFunNames(obj.factorDBName{i});
                tmpFlag = ismember(tmpFunNames, factorName);
                if any(tmpFlag)
                    dbName = obj.factorDBName{i};
                    return
                end
            end
            error('在因子数据库中未找到该factor')
        end
        function [flag, info] = checkFactor(obj)
            %CHECKFACTOR % 检查factorFunPath下的因子是否都已经在数据库中建立了表
            %    输入参数
            %    无
            %
            %    输出参数
            %    flag       bool变量
            %    如果flag==1,则说明factorFunPath下的因子都已经在数据库中建立了表,此时info为空
            %    如果flag==0,则说明存在因子没有在数据库中建表
            %    info       n*2形状的元胞数组,装有没有在数据库中建表的factorFunPath下的因子
            %    第一列为数据库名, 第二列为因子名
            
            %    检查factorFunPath下的因子是否都已经在数据库中建立了表
            
            %%
            dbTables = obj.qryDBTables();
            factorInDBNotInFun = containers.Map();
            factorInFunNotInDB = containers.Map();
            for i = 1:length(obj.factorDBName)
                tmpFactorInFun = obj.factorFunNames(obj.factorDBName{i})';
                tmpFactorInDB = dbTables(obj.factorDBName{i});
                flag1 = ismember(tmpFactorInFun, tmpFactorInDB);
                flag2 = ismember(tmpFactorInDB, tmpFactorInFun);
                if any(~flag1)
                    factorInFunNotInDB(obj.factorDBName{i}) = tmpFactorInFun(~flag1);
                end
                if any(~flag2)
                    factorInDBNotInFun(obj.factorDBName{i}) = tmpFactorInFun(~flag2);
                end
            end
            if ~isempty(factorInDBNotInFun)
                cellArray = map2cell(factorInDBNotInFun);
                tmpStr = cell(size(cellArray, 1), 1);
                for i = 1:length(tmpStr)
                    tmpStr{i} = sprinf('数据库 %s ：\t  表%s\n',cellArray{i, 1}, cellArray{i, 2});
                end
                error(['数据库中的下列表缺少对应的更新函数,请检查对应的函数文件夹\n',strjoin(tmpStr, '')])
            end
            flag = isempty(factorInFunNotInDB);
            if nargout == 2
                info = map2cell(factorInFunNotInDB);
            end
        end
        function private_createSingleIndex(obj, factorName)
            %PRIVATE_CREATESINGLEINDEX 创建数据库里的单个因子表的索引,私有方法
            %    输入参数
            %    factorName   字符串格式的因子名,如'FACTOR_SP_TTM'
            %
            %    创建数据库里的单个因子表的索引
            %    该方法不应该被用户直接用于创建数据库里的因子表的索引,因为该方法没有对数据库操作进行提交确认
            %    该方法应该在其他供用户执行创建索引的操作的函数中被调用
            
            %%
            % 找到因子在哪个数据库里
            dbName = obj.findFactorBelongToDB(factorName);
            % 因子所在数据库的连接
            tmpFactorDB = obj.factorDBConn(dbName);
            % 在数据库中建立好索引
            fprintf('正在创建数据库%s中的因子%s的索引\n', dbName, factorName);
            createCodeTradedateIndex(tmpFactorDB, factorName);
            fprintf('数据库%s中的因子%s的索引已创建\n', dbName, factorName);
        end
        function private_dropSingleIndex(obj, factorName)
            %PRIVATE_DROPSINGLEINDEX 删除数据库里的单个因子表的索引,私有方法
            %    输入参数
            %    factorName   字符串格式的因子名,如'FACTOR_SP_TTM'
            %
            %    删除数据库里的单个因子表的索引
            %    该方法不应该被用户直接用于删除数据库里的因子表的索引,因为该方法没有对数据库操作进行提交确认
            %    该方法应该在其他供用户执行删除索引的操作的函数中被调用
            
            %%
            % 找到因子在哪个数据库里
            dbName = obj.findFactorBelongToDB(factorName);
            % 因子所在数据库的连接
            tmpFactorDB = obj.factorDBConn(dbName);
            % 在数据库中删除索引
            fprintf('正在删除数据库%s中的因子%s的索引\n', dbName, factorName);
            dropCodeTradedateIndex(tmpFactorDB, factorName);
            fprintf('数据库%s中的因子%s的索引已删除\n', dbName, factorName);
        end
        function private_createSingleTable(obj, startDate, endDate, factorName)
            %PRIVATE_CREATESINGLETABLE 创建数据库里的单个因子,私有方法
            %    输入参数
            %    startDate    开始日期
            %    endDate      结束日期
            %    factorName   字符串格式的因子名,如'FACTOR_SP_TTM'
            %
            %    创建数据库里的单个因子
            %    该方法不应该被用户直接用于创建数据中的表,因为该方法没有对数据库操作进行提交确认
            %    该方法应该在其他供用户执行创建表的操作的函数中被调用
            %    创建表需要开始日期及结束日期
            %    仅适用于数据库中的该因子没有建表的情况
            %    如果数据库中已经有建好的表,该函数会报错,错误为table already exists
            
            %%
            % 计算因子数据的函数句柄
            tmpFactorFunHandle = str2func(factorName);
            % 因子所在数据库名称
            dbName = obj.findFactorBelongToDB(factorName);
            % 因子所在数据库的连接
            tmpFactorDB = obj.factorDBConn(dbName);
            % 在数据库中建立好对应的表
            fprintf('正在创建数据库%s中的因子%s\n', dbName, factorName);
            createFactor(tmpFactorDB, factorName);
            % 将时间分成时间段,因为过长时间的数据内存无法同时容纳,必须分开进行插入数据操作
            timeInteval = timeSplit(startDate, endDate);
            fprintf('开始更新数据库%s中的因子%s,开始时间为%s结束时间为%s\n分时间段插入\n', dbName, factorName,...
                startDate, endDate);
            for i = 1:length(timeInteval)
                tmpStartDate = timeInteval{i}.startDate;
                tmpEndDate = timeInteval{i}.endDate;
                fprintf('正在向数据库%s中插入因子%s,开始时间为%s结束时间为%s\n', dbName, factorName,...
                    tmpStartDate, tmpEndDate);
                % 取出这段时间的因子数据
                rawData = tmpFactorFunHandle(obj.windDBConn, tmpStartDate, tmpEndDate);
                % 将因子数据插入到数据库中
                insertFactor(tmpFactorDB, factorName, rawData);
            end
        end
        % 更新单张表,私有
        function private_updateSingleTable(obj, endDate, factorName)
            %UPDATESINGLETABLE 更新数据库单个因子,私有方法
            %    输入参数
            %    endDate      结束日期
            %    factorName   字符串格式的因子名,如'FACTOR_SP_TTM'
            %
            %    更新数据库单个因子
            %    该方法不应该被用户直接用于更新数据中的表,因为该方法没有对数据库操作进行提交确认
            %    该方法应该在其他供用户执行更新表的操作的函数中被调用
            %    更新表只需要结束日期,开始日期会自动生成
            %    仅会更新数据库中所有已创建的表,如果该表未创建,不会先创建在更新该表
            
            %%
            % 更新时先检查该表的最新数据的日期
            lastUpdate = obj.qryLastUpdate(factorName);
            % 新插入数据的开始时间要是最新数据的日期的后一天
            startDate = datestr(datenum(lastUpdate, 'yyyymmdd')+1, 'yyyymmdd');
            
            % 因子所在数据库名称
            dbName = obj.findFactorBelongToDB(factorName);
            if datenum(startDate, 'yyyymmdd') > datenum(endDate, 'yyyymmdd')
                fprintf('数据库%s中的因子%s已经更新至日期%s,无需更新\n', dbName, factorName, lastUpdate);
                return
            end
            % 计算因子数据的函数句柄
            tmpFactorFunHandle = str2func(factorName);
            % 因子所在数据库的连接
            tmpFactorDB = obj.factorDBConn(dbName);
            % 将时间分成时间段,因为过长时间的数据内存无法同时容纳,必须分开进行插入数据操作
            timeInteval = timeSplit(startDate, endDate);
            fprintf('开始更新数据库%s中的因子%s,开始时间为%s结束时间为%s\n分时间段插入\n', dbName, factorName,...
                startDate, endDate);
            for i = 1:length(timeInteval)
                tmpStartDate = timeInteval{i}.startDate;
                tmpEndDate = timeInteval{i}.endDate;
                fprintf('正在向数据库%s中插入因子%s,开始时间为%s结束时间为%s\n', dbName, factorName,...
                    tmpStartDate, tmpEndDate);
                % 取出这段时间的因子数据
                rawData = tmpFactorFunHandle(obj.windDBConn, tmpStartDate, tmpEndDate);
                % 将因子数据插入到数据库中
                insertFactor(tmpFactorDB, factorName, rawData);
            end
        end
        function [laseUpdate] = qryLastUpdate(obj, facName)
            %QRYLASTUPDATE 查询某个因子的最后更新日期
            %   输入参数
            %   facName   因子名称
            %
            %   查询某个因子的最后更新日期
            %   会返回因子最后的日期的8位日期字符串，如'20090101'
            
            %%
            % 因子所在数据库名称
            dbName = obj.findFactorBelongToDB(facName);
            % 因子所在数据库的连接
            tmpFactorDB = obj.factorDBConn(dbName);
            % 查询数据库中因子最后的时间
            sqlstr = sprintf('SELECT MAX(tradedate) FROM %s', facName);
            tmp = DBFetch(tmpFactorDB, sqlstr);
            laseUpdate = tmp{1};
        end
        function  cleanDB(obj)
            %CLEANDB 清理数据库
            %    输入参数
            %    无
            %
            %    清理数据库
            
            %%
            for i = 1:length(obj.factorDBName)
                tmp = tables(obj.factorDBConn(obj.factorDBName{i}));
                tableNames = tmp(ismember(tmp(:, 2), 'TABLE'), 1);
                for j = 1:length(tableNames)
                    sqlstr = sprintf('PRAGMA table_info(%s)', tableNames{j});
                    tmpTableInfo = DBFetch(obj.factorDBConn(obj.factorDBName{i}), sqlstr, 'table');
                    if any(ismember(tmpTableInfo.name, 'tradedate'))
                        dateCol = 'tradedate';
                    elseif  any(ismember(tmpTableInfo.name, 'issuingdate'))
                        dateCol = 'issuingdate';
                    else
                        error('数据库表内没有tradedate或issuingdate的日期列');
                    end
                    sqlstr = sprintf('DELETE FROM %s WHERE %s = ''No Data''',  tableNames{j},dateCol);
                    exec(obj.factorDBConn(obj.factorDBName{i}), sqlstr);
                    
                end
            end
        end
    end
end

# 在这里加一行代码