classdef Factor < handle
    %FACOTR ���ڲ��������������ݿ����
    %   LAST UPDATE 2017/7/13 BY BIG HUANG
    %   FACOTR properties:
    %   public:
    %         DBPath                                - ���ݿ��ļ�����·��
    %         factorFunPath                         - ���Ӻ�������·��
    %   private:
    %         factorDBConn                          - ��DBPath�ļ��������ݿ������,map��ʽ
    %                                                 ��Ϊ���ݿ������ַ���,ֵΪ���Ӧ���ݵ�����
    %         factorDBName                          - DBPath�ļ��������ݿ�������ַ���,cell��ʽ
    %         factorFunHandle                       - factorFunPath�ļ����µ����Ӻ����ľ��,map��ʽ
    %                                                 ��Ϊ���ݿ������ַ���,ֵΪһ��Ԫ��,�����и����ݿ����������ӵĺ������
    %         factorFunNames                        - factorFunPath�ļ����µ����Ӻ���������,map��ʽ
    %                                                 ��Ϊ���ݿ������ַ���,ֵΪһ��Ԫ��,�����и����ݿ����������ӵĺ�������
    %         windDBConn                            - ��wind���ݿ������
    %
    %   FACOTR methods and functions:
    %   public:
    %     ������:
    %         Factor                                - ���캯��,��ʼ��Factor����
    %     �������²�����
    %         createAllTable                        - �������ݿ����е�����
    %         updateAllTable                        - �������ݿ����е�����
    %         createSingleTable                     - �������ݿ���ĵ�������
    %         updateSingleTable                     - �������ݿ���ĵ�������
    %         oneClickUpdate                        - һ���������ݿ���������,��������δ����������
    %         createAllIndex                        - �������ݿ����е����ӵ�����
    %         dropAllIndex                          - ɾ�����ݿ����е����ӵ�����
    %     ��ѯ���״̬��
    %         qryDBInfo                             - ��ѯ���ݿ����Ѵ��ڵı����Ϣ
    %         qryDBTables                           - ��ѯ���ݿ��е�ǰ����Щ��
    %     ������
    %         closeAllConn                          - �ر����������ݿ������
    %   private:
    %     ��ʼ�����������ԣ�
    %         setFactorDBConn                       - ��������factorDBConn
    %         setFactorDBName                       - ��������factorDBName
    %         setFactorFunHandleAndFactorFunNames   - ��������factorFunHandle��FactorFunNames
    %         setWindDBConn                         - ��������windDBConn
    %         addFactorFunPath                      - �����Ӻ�����·����ӵ�����·��
    %     ������
    %         checkFactor                           - ���factorFunPath�µ������Ƿ��Ѿ������ݿ��н����˱�
    %         commitDB                              - �ύ���ݿ�����
    %         rollbackDB                            - �ع����ݿ����
    %         findFactorBelongToDB                  - �ҵ�ָ�������������ĸ����ݿ�,
    %         private_createSingleTable             - �������ݿ���ĵ�������,˽�з���
    %         private_updateSingleTable             - �������ݿ���ĵ�������,˽�з���
    %         private_createSingleIndex             - �������ݿ���ĵ������ӵ�����,˽�з���
    %         private_dropSingleIndex               - ɾ�����ݿ���ĵ������ӵ�����,˽�з���
    %         qryLastUpdate                         - ��ѯĳ�����ӵ�����������
    %         cleanDB                               - �������ݿ�
    
    %% ��������
    properties(Access = 'public')
        % ���ݿ��ļ�����·��
        DBPath
        % ���Ӻ�������·��
        factorFunPath
    end
    
    %% ˽������
    properties(Access = 'private', Hidden = true)
        % ��DBPath�ļ��������ݿ������,map��ʽ
        % ��Ϊ���ݿ������ַ���,ֵΪ���Ӧ���ݵ�����
        factorDBConn
        % DBPath�ļ��������ݿ�������ַ���,cell��ʽ
        factorDBName
        % factorFunPath�ļ����µ����Ӻ����ľ��,map��ʽ
        % ��Ϊ���ݿ������ַ���,ֵΪһ��Ԫ��,�����и����ݿ����������ӵĺ������
        factorFunHandle
        % factorFunPath�ļ����µ����Ӻ���������,map��ʽ
        % ��Ϊ���ݿ������ַ���,ֵΪһ��Ԫ��,�����и����ݿ����������ӵĺ�������
        factorFunNames
        % ��wind���ݿ������
        windDBConn
    end
    
    %% ���з���
    methods(Access = 'public')
        function obj = Factor(DBPath, factorFunPath, linkOrNot)
            %FACTOR ���캯��,��ʼ��Factor����
            %   �������
            %   DBPath          ���ݿ��·��
            %   factorFunPath   ���Ӻ������ļ���·��
            %
            %   ��ѡ����
            %   linkOrNot       �Ƿ�����wind���ؿ�,Ĭ��Ϊ��,�����������޷������ݿ���и���
            %
            %   ����һ��Factor����,�û�����ʹ��Factor�����һϵ�з��������ݿ���и��²���
            %%
            if nargin == 2
                linkOrNot = true;
            end
            % ��������DBPath
            obj.DBPath = DBPath;
            % ��������factorFunPath
            obj.factorFunPath = factorFunPath;
            % ��������factorDBName
            obj.setFactorDBName()
            % ��������factorDBConn
            obj.setFactorDBConn()
            % ��������factorFunHandle
            obj.setFactorFunHandleAndFactorFunNames()
            % ��������windDBConn
            obj.setWindDBConn(linkOrNot);
            % �����Ӻ�����·����ӵ�����·��
            obj.addFactorFunPath()
            % �ع����ݿ����
            obj.rollbackDB()
        end  
        function oneClickUpdate(obj, startDate, endDate)
            %ONECLICKUPDATE һ���������ݿ���������,��������δ����������
            %    �������
            %    startDate    ��ʼ����
            %    endDate      ��������
            %
            %    һ���������ݿ���������,��������δ����������
            %    ������Ӻ����е����Ӷ����Ѿ������ݿ��н��������
            %    ������д��һ�����ӵĺ���,����factorFunPath·���µ��ļ�����
            %    �ú����ڸ���ʱ���Զ����δ�����ݿ��н��������
            %    �ȸ������ݿ������еı�,�ٽ��µ����������ݿ��н���,������
            
            %%
            % �ع����ݿ����
            obj.rollbackDB()
            fprintf('��ʼ������ݿ������еı�\n');
            dbTables = obj.qryDBTables();
            
            tmpKeys = dbTables.keys;
            % �������еı�
            for i = 1:length(dbTables)
                tmpFactors = dbTables(tmpKeys{i});
                if ~isempty(dbTables(tmpKeys{i}))
                    for j = 1:size(tmpFactors, 1)
                        obj.private_updateSingleTable(endDate, tmpFactors{j, 1});
                        obj.commitDB();
                    end
                end
            end
            
            % ������ݿ�������Щ��û����
            [flag, info] = obj.checkFactor();
            % �����������µı�
            if ~flag
                fprintf('��ʼ�������ݿ���δ�����ı�\n');
                fprintf('�¼�����������\n')
                for i = 1:size(info, 1)
                    fprintf('���ݿ�%s �е�����%s\n', info{i, 1}, info{i, 2});
                end
                for i = 1:size(info, 1)
                    obj.private_createSingleTable(startDate, endDate, info{i, 2})
                    obj.commitDB();
                end
            end
            % �ύȷ�����ݿ����
            obj.cleanDB()
            obj.commitDB();
        end
        function createAllTable(obj, startDate, endDate)
            %CREATEALLTABLE �������ݿ����е�����
            %    �������
            %    startDate    ��ʼ����
            %    endDate      ��������
            %
            %    �������ݿ������е�����
            %    ��������Ҫ��ʼ���ڼ���������
            %    �����������ݿ��е����Ӷ�û�н�������
            %    ������ݿ����Ѿ��н��õı�,�ú����ᱨ��,����Ϊtable already exists
            
            %%
            obj.rollbackDB()
            fprintf('��ʼ�������ݿ������еı�\n');
            for i = 1:length(obj.factorDBName)
                factorNames = obj.factorFunNames(obj.factorDBName{i});
                for j = 1:length(factorNames)
                    obj.private_createSingleTable(startDate, endDate, factorNames{j});
                end
            end
            obj.commitDB();
        end
        function updateAllTable(obj, endDate)
            %UPDATEALLTABLE �������ݿ����е�����
            %    �������
            %    endDate      ��������
            %
            %    �������ݿ������е�����
            %    ���±�ֻ��Ҫ��������,��ʼ���ڻ��Զ�����
            %    ����������ݿ��������Ѵ����ı�,����ñ�δ����,�����ȴ����ڸ��¸ñ�
            
            %%
            obj.rollbackDB()
            fprintf('��ʼ�������ݿ������еı�\n');
            for i = 1:length(obj.factorDBName)
                factorNames = obj.factorFunNames(obj.factorDBName{i});
                for j = 1:length(factorNames)
                    obj.private_updateSingleTable(endDate, factorNames{j});
                end
            end
            obj.commitDB();
        end
        function createAllIndex(obj)
            %CREATEALLINDEX �������ݿ��������ӵ�����
            %    �������
            %    ��
            %
            %    �������ݿ��������ӵ�����
            %    �����������ݿ��е����Ӷ�û�н������������
            %    ������ݿ��еı��Ѿ���������,�ú����ᱨ��,����Ϊindex already exists
            
            %%
            obj.rollbackDB()
            fprintf('��ʼ�������ݿ������еı������\n');
            for i = 1:length(obj.factorDBName)
                factorNames = obj.factorFunNames(obj.factorDBName{i});
                for j = 1:length(factorNames)
                    obj.private_createSingleIndex(factorNames{j})
                end
            end
            obj.commitDB();
        end
        function dropAllIndex(obj)
            %DROPALLINDEX �������ݿ��������ӵ�����
            %    �������
            %    ��
            %
            %    ɾ�����ݿ��������ӵ�����
            
            %%
            obj.rollbackDB()
            fprintf('��ʼɾ�����ݿ������е�����\n');
            for i = 1:length(obj.factorDBName)
                factorNames = obj.factorFunNames(obj.factorDBName{i});
                for j = 1:length(factorNames)
                    obj.private_dropSingleIndex(factorNames{j})
                end
            end
            obj.commitDB();
        end
        function createSingleTable(obj, startDate, endDate, factorName)
            %CREATESINGLETABLE �������ݿ���ĵ�������
            %    �������
            %    startDate    ��ʼ����
            %    endDate      ��������
            %    factorName   �ַ�����ʽ��������,��'FACTOR_SP_TTM'
            %
            %    �������ݿ���ĵ�������
            %    ��������Ҫ��ʼ���ڼ���������
            %    �����������ݿ��еĸ�����û�н�������
            %    ������ݿ����Ѿ��н��õı�,�ú����ᱨ��,����Ϊtable already exists
            
            %%
            obj.rollbackDB();
            obj.private_createSingleTable(startDate, endDate, factorName);
            obj.commitDB();
        end
        function updateSingleTable(obj, endDate, factorName)
            %UPDATESINGLETABLE �������ݿⵥ������
            %    �������
            %    endDate      ��������
            %    factorName   �ַ�����ʽ��������,��'FACTOR_SP_TTM'
            %
            %    �������ݿⵥ������
            %    ���±�ֻ��Ҫ��������,��ʼ���ڻ��Զ�����
            %    ����������ݿ��������Ѵ����ı�,����ñ�δ����,�����ȴ����ڸ��¸ñ�
            
            %%
            obj.rollbackDB();
            obj.private_updateSingleTable(endDate, factorName);
            obj.commitDB();
        end
        function dbTables = qryDBTables(obj)
            %QRYDBTABLES ��ѯ���ݿ��е�ǰ����Щ��
            %    �������
            %    ��
            %
            %    ��ѯ���ݿ��е�ǰ����Щ��
            %    ����һ��map��ʽ�ı���,��Ϊ���ݿ������,ֵΪ��Ӧ�����ݿ�������Щ��
            
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
            %QRYDBINFO ��ѯ���ݿ����Ѵ��ڵı����Ϣ
            %    �������
            %    ��
            %
            %    ��ѯ���ݿ����Ѵ��ڵı����Ϣ
            %    ����һ��table,ÿ�зֱ�Ϊ��
            %    ������,���ڵ����ݿ�����,���¿�ʼʱ��,���½���ʱ��
            
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
                        error('���ݿ����û��tradedate��issuingdate��������');
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
            %CLOSEALLCONN �ر����������ݿ������
            %    �������
            %    ��
            %
            %    �ر����������ݿ������
            %    �������������ݿ�����Ӻ���wind���ݿ������
            
            %%
            
            % �ر���wind���ݿ������
            close(obj.windDBConn);
            for i = 1:length(obj.factorDBConn)
                % �ص���Ӧ�������ݿ������
                close(obj.factorDBConn(obj.factorDBName{i}));
            end
        end
    end
    
    %% ˽�з���
    methods(Access = 'private', Hidden = true)
        function setFactorDBName(obj)
            %SETFACTORDBNAME ��������factorDBName
            %    �������
            %    ��
            %
            %    ��������factorDBName
            %    �������ݿ�����
            %    ���ݿ�����Ƽ�ΪfactorFunPath·���µĸ����ļ�������
            
            %%
            % ��ȡfactorFunPath·���µĸ����ļ�������,�������ݿ�����
            obj.factorDBName = readFolder(obj.factorFunPath, '');
        end
        % ��������factorDBConn
        function setFactorDBConn(obj)
            %SETFACTORDBCONN % ��������factorDBConn
            %    �������
            %    ��
            %
            %    ��������factorDBConn
            %    �������������ݿ������
            %    ���ݿ�����Ƽ�ΪfactorFunPath·���µĸ����ļ�������
            
            %%
            obj.factorDBConn = containers.Map();
            for i = 1:length(obj.factorDBName)
                obj.factorDBConn(obj.factorDBName{i}) = DBlinkFACTOR(obj.DBPath, [obj.factorDBName{i}, '.db']);
            end
        end
        function setFactorFunHandleAndFactorFunNames(obj)
            %SETFACTORFUNHANDLEANDFACTORFUNNAMES % ��������factorFunHandle��FactorFunNames
            %    �������
            %    ��
            %
            %    ��������factorFunHandle��factorFunNames
            %    ��ȡ���Ӻ��������ƺ����Ӻ����ĺ������
            %    factorFunNames ��ΪfactorFunPath·���µĸ����ļ����е�m�ļ�������
            %    factorFunHandle ��ΪfactorFunPath·���µĸ����ļ����е�m�ļ�������ת���ɵĺ������
            
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
            %SETWINDDBCONN % ��������windDBConn
            %    �������
            %    linkOrNot       �Ƿ�����wind���ؿ�,Ĭ��Ϊ��,�����������޷������ݿ���и���
            %
            %    ��������windDBConn
            %    ������wind���ݿ������
            
            %%
            if linkOrNot
                obj.windDBConn = DBlinkWind();
            else
                warning('δ��������wind���ݿ�,���޷����и������ݿ�Ĳ���')
                obj.windDBConn = '';
            end
            
        end
        function addFactorFunPath(obj)
            %ADDFACTORFUNPATH % �����Ӻ�����·����ӵ�����·��
            %    �������
            %    ��
            %
            %    �����Ӻ�����·����ӵ�����·��
            %    Ҳ����factorFunPath
            %    ����֮��������Ӻ����ľ��ʱ�ſ�������
            
            %%
            for i = 1:length(obj.factorDBName)
                addpath([obj.factorFunPath,'\',obj.factorDBName{i}])
            end
        end
        function commitDB(obj)
            %COMMITDB �ύ���ݿ�����
            %    �������
            %    ��
            %
            %    �ύ���ݿ�����
            %    ȷ��֮ǰ�����ݿ�����и���
            %%
            for i = 1:length(obj.factorDBConn)
                commit(obj.factorDBConn(obj.factorDBName{i}));
            end
        end
        function rollbackDB(obj)
            %ROLLBACKDB �ع����ݿ����
            %    �������
            %    ��
            %
            %    �ع����ݿ����
            %    ��δ�ύ�����ݿ�����ȡ��
            
            %%
            for i = 1:length(obj.factorDBConn)
                % �ع���Ӧ���ݿ�
                rollback(obj.factorDBConn(obj.factorDBName{i}));
            end
        end
        function dbName = findFactorBelongToDB(obj, factorName)
            %FINDFACTORBELONGTODB % �ҵ�ָ�������������ĸ����ݿ�,
            %    �������
            %    ��
            %
            %    �ҵ�ָ�������������ĸ����ݿ�
            %    Ҳ��������ͬ���ĺ�����factorFunPath�µ��ĸ��ļ���
            
            %%
            for i = 1:length(obj.factorDBName)
                tmpFunNames = obj.factorFunNames(obj.factorDBName{i});
                tmpFlag = ismember(tmpFunNames, factorName);
                if any(tmpFlag)
                    dbName = obj.factorDBName{i};
                    return
                end
            end
            error('���������ݿ���δ�ҵ���factor')
        end
        function [flag, info] = checkFactor(obj)
            %CHECKFACTOR % ���factorFunPath�µ������Ƿ��Ѿ������ݿ��н����˱�
            %    �������
            %    ��
            %
            %    �������
            %    flag       bool����
            %    ���flag==1,��˵��factorFunPath�µ����Ӷ��Ѿ������ݿ��н����˱�,��ʱinfoΪ��
            %    ���flag==0,��˵����������û�������ݿ��н���
            %    info       n*2��״��Ԫ������,װ��û�������ݿ��н����factorFunPath�µ�����
            %    ��һ��Ϊ���ݿ���, �ڶ���Ϊ������
            
            %    ���factorFunPath�µ������Ƿ��Ѿ������ݿ��н����˱�
            
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
                    tmpStr{i} = sprinf('���ݿ� %s ��\t  ��%s\n',cellArray{i, 1}, cellArray{i, 2});
                end
                error(['���ݿ��е����б�ȱ�ٶ�Ӧ�ĸ��º���,�����Ӧ�ĺ����ļ���\n',strjoin(tmpStr, '')])
            end
            flag = isempty(factorInFunNotInDB);
            if nargout == 2
                info = map2cell(factorInFunNotInDB);
            end
        end
        function private_createSingleIndex(obj, factorName)
            %PRIVATE_CREATESINGLEINDEX �������ݿ���ĵ������ӱ������,˽�з���
            %    �������
            %    factorName   �ַ�����ʽ��������,��'FACTOR_SP_TTM'
            %
            %    �������ݿ���ĵ������ӱ������
            %    �÷�����Ӧ�ñ��û�ֱ�����ڴ������ݿ�������ӱ������,��Ϊ�÷���û�ж����ݿ���������ύȷ��
            %    �÷���Ӧ�����������û�ִ�д��������Ĳ����ĺ����б�����
            
            %%
            % �ҵ��������ĸ����ݿ���
            dbName = obj.findFactorBelongToDB(factorName);
            % �����������ݿ������
            tmpFactorDB = obj.factorDBConn(dbName);
            % �����ݿ��н���������
            fprintf('���ڴ������ݿ�%s�е�����%s������\n', dbName, factorName);
            createCodeTradedateIndex(tmpFactorDB, factorName);
            fprintf('���ݿ�%s�е�����%s�������Ѵ���\n', dbName, factorName);
        end
        function private_dropSingleIndex(obj, factorName)
            %PRIVATE_DROPSINGLEINDEX ɾ�����ݿ���ĵ������ӱ������,˽�з���
            %    �������
            %    factorName   �ַ�����ʽ��������,��'FACTOR_SP_TTM'
            %
            %    ɾ�����ݿ���ĵ������ӱ������
            %    �÷�����Ӧ�ñ��û�ֱ������ɾ�����ݿ�������ӱ������,��Ϊ�÷���û�ж����ݿ���������ύȷ��
            %    �÷���Ӧ�����������û�ִ��ɾ�������Ĳ����ĺ����б�����
            
            %%
            % �ҵ��������ĸ����ݿ���
            dbName = obj.findFactorBelongToDB(factorName);
            % �����������ݿ������
            tmpFactorDB = obj.factorDBConn(dbName);
            % �����ݿ���ɾ������
            fprintf('����ɾ�����ݿ�%s�е�����%s������\n', dbName, factorName);
            dropCodeTradedateIndex(tmpFactorDB, factorName);
            fprintf('���ݿ�%s�е�����%s��������ɾ��\n', dbName, factorName);
        end
        function private_createSingleTable(obj, startDate, endDate, factorName)
            %PRIVATE_CREATESINGLETABLE �������ݿ���ĵ�������,˽�з���
            %    �������
            %    startDate    ��ʼ����
            %    endDate      ��������
            %    factorName   �ַ�����ʽ��������,��'FACTOR_SP_TTM'
            %
            %    �������ݿ���ĵ�������
            %    �÷�����Ӧ�ñ��û�ֱ�����ڴ��������еı�,��Ϊ�÷���û�ж����ݿ���������ύȷ��
            %    �÷���Ӧ�����������û�ִ�д�����Ĳ����ĺ����б�����
            %    ��������Ҫ��ʼ���ڼ���������
            %    �����������ݿ��еĸ�����û�н�������
            %    ������ݿ����Ѿ��н��õı�,�ú����ᱨ��,����Ϊtable already exists
            
            %%
            % �����������ݵĺ������
            tmpFactorFunHandle = str2func(factorName);
            % �����������ݿ�����
            dbName = obj.findFactorBelongToDB(factorName);
            % �����������ݿ������
            tmpFactorDB = obj.factorDBConn(dbName);
            % �����ݿ��н����ö�Ӧ�ı�
            fprintf('���ڴ������ݿ�%s�е�����%s\n', dbName, factorName);
            createFactor(tmpFactorDB, factorName);
            % ��ʱ��ֳ�ʱ���,��Ϊ����ʱ��������ڴ��޷�ͬʱ����,����ֿ����в������ݲ���
            timeInteval = timeSplit(startDate, endDate);
            fprintf('��ʼ�������ݿ�%s�е�����%s,��ʼʱ��Ϊ%s����ʱ��Ϊ%s\n��ʱ��β���\n', dbName, factorName,...
                startDate, endDate);
            for i = 1:length(timeInteval)
                tmpStartDate = timeInteval{i}.startDate;
                tmpEndDate = timeInteval{i}.endDate;
                fprintf('���������ݿ�%s�в�������%s,��ʼʱ��Ϊ%s����ʱ��Ϊ%s\n', dbName, factorName,...
                    tmpStartDate, tmpEndDate);
                % ȡ�����ʱ�����������
                rawData = tmpFactorFunHandle(obj.windDBConn, tmpStartDate, tmpEndDate);
                % ���������ݲ��뵽���ݿ���
                insertFactor(tmpFactorDB, factorName, rawData);
            end
        end
        % ���µ��ű�,˽��
        function private_updateSingleTable(obj, endDate, factorName)
            %UPDATESINGLETABLE �������ݿⵥ������,˽�з���
            %    �������
            %    endDate      ��������
            %    factorName   �ַ�����ʽ��������,��'FACTOR_SP_TTM'
            %
            %    �������ݿⵥ������
            %    �÷�����Ӧ�ñ��û�ֱ�����ڸ��������еı�,��Ϊ�÷���û�ж����ݿ���������ύȷ��
            %    �÷���Ӧ�����������û�ִ�и��±�Ĳ����ĺ����б�����
            %    ���±�ֻ��Ҫ��������,��ʼ���ڻ��Զ�����
            %    ����������ݿ��������Ѵ����ı�,����ñ�δ����,�����ȴ����ڸ��¸ñ�
            
            %%
            % ����ʱ�ȼ��ñ���������ݵ�����
            lastUpdate = obj.qryLastUpdate(factorName);
            % �²������ݵĿ�ʼʱ��Ҫ���������ݵ����ڵĺ�һ��
            startDate = datestr(datenum(lastUpdate, 'yyyymmdd')+1, 'yyyymmdd');
            
            % �����������ݿ�����
            dbName = obj.findFactorBelongToDB(factorName);
            if datenum(startDate, 'yyyymmdd') > datenum(endDate, 'yyyymmdd')
                fprintf('���ݿ�%s�е�����%s�Ѿ�����������%s,�������\n', dbName, factorName, lastUpdate);
                return
            end
            % �����������ݵĺ������
            tmpFactorFunHandle = str2func(factorName);
            % �����������ݿ������
            tmpFactorDB = obj.factorDBConn(dbName);
            % ��ʱ��ֳ�ʱ���,��Ϊ����ʱ��������ڴ��޷�ͬʱ����,����ֿ����в������ݲ���
            timeInteval = timeSplit(startDate, endDate);
            fprintf('��ʼ�������ݿ�%s�е�����%s,��ʼʱ��Ϊ%s����ʱ��Ϊ%s\n��ʱ��β���\n', dbName, factorName,...
                startDate, endDate);
            for i = 1:length(timeInteval)
                tmpStartDate = timeInteval{i}.startDate;
                tmpEndDate = timeInteval{i}.endDate;
                fprintf('���������ݿ�%s�в�������%s,��ʼʱ��Ϊ%s����ʱ��Ϊ%s\n', dbName, factorName,...
                    tmpStartDate, tmpEndDate);
                % ȡ�����ʱ�����������
                rawData = tmpFactorFunHandle(obj.windDBConn, tmpStartDate, tmpEndDate);
                % ���������ݲ��뵽���ݿ���
                insertFactor(tmpFactorDB, factorName, rawData);
            end
        end
        function [laseUpdate] = qryLastUpdate(obj, facName)
            %QRYLASTUPDATE ��ѯĳ�����ӵ�����������
            %   �������
            %   facName   ��������
            %
            %   ��ѯĳ�����ӵ�����������
            %   �᷵�������������ڵ�8λ�����ַ�������'20090101'
            
            %%
            % �����������ݿ�����
            dbName = obj.findFactorBelongToDB(facName);
            % �����������ݿ������
            tmpFactorDB = obj.factorDBConn(dbName);
            % ��ѯ���ݿ�����������ʱ��
            sqlstr = sprintf('SELECT MAX(tradedate) FROM %s', facName);
            tmp = DBFetch(tmpFactorDB, sqlstr);
            laseUpdate = tmp{1};
        end
        function  cleanDB(obj)
            %CLEANDB �������ݿ�
            %    �������
            %    ��
            %
            %    �������ݿ�
            
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
                        error('���ݿ����û��tradedate��issuingdate��������');
                    end
                    sqlstr = sprintf('DELETE FROM %s WHERE %s = ''No Data''',  tableNames{j},dateCol);
                    exec(obj.factorDBConn(obj.factorDBName{i}), sqlstr);
                    
                end
            end
        end
    end
end

# �������һ�д���