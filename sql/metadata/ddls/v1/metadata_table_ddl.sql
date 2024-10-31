--https://dev.mysql.com/doc/refman/8.4/en/timestamp-initialization.html

create table  fwk_meta.project(
    project_id int identity(1,1) primary key,
    project_name varchar(255) not null,
    domain varchar(100),
    project_description text,
    project_property varchar(255),
    is_active_flag int,
    priority int,
    created_time datetime DEFAULT GETDATE(),
    updated_time datetime DEFAULT GETDATE(),
    createdby varchar(100) DEFAULT SYSTEM_USER,
    updatedby varchar(100) DEFAULT SYSTEM_USER
);


create table  fwk_meta.app (
    app_id int identity(1,1) primary key,
    app_name varchar(255),
    app_type varchar(100),
    app_property nvarchar(max) NOT NULL,
    is_active_flag int,
    created_time datetime DEFAULT GETDATE(),
    updated_time datetime DEFAULT GETDATE(),
    createdby varchar(100) DEFAULT SYSTEM_USER,
    updatedby varchar(100) DEFAULT SYSTEM_USER
);

create table  fwk_meta.module(
    module_id int identity(1,1) primary key,
    data_layer varchar(100),
    project_id int,
    module_description text,
    is_active_flag int,
    priority int,
    created_time datetime DEFAULT GETDATE(),
    updated_time datetime DEFAULT GETDATE(),
    createdby varchar(100) DEFAULT SYSTEM_USER,
    updatedby varchar(100) DEFAULT SYSTEM_USER,
    foreign key (project_id) references project(project_id)
);

create table  fwk_meta.task(
    task_id int identity(1,1) primary key,
    module_id int,
    source_entity int not null,
    target_entity int not null,
    app_id int,
    task_property varchar(255),
    watermark_value varchar(255),
    is_active_flag int,
    priority int,
    created_time datetime DEFAULT GETDATE(),
    updated_time datetime DEFAULT GETDATE(),
    createdby varchar(100) DEFAULT SYSTEM_USER,
    updatedby varchar(100) DEFAULT SYSTEM_USER,
    foreign key (module_id) references module(module_id),
    foreign key (app_id) references app(app_id)
);

create table  fwk_meta.entity_source (
    entity_source_id int identity(1,1) primary key,
    source_name varchar(255),
    source_type varchar(100),
    source_property nvarchar(max),
    is_active_flag int,
    created_time datetime DEFAULT GETDATE(),
    updated_time datetime DEFAULT GETDATE(),
    createdby varchar(100) DEFAULT SYSTEM_USER,
    updatedby varchar(100) DEFAULT SYSTEM_USER
);

create table  fwk_meta.entity (
    entity_id int identity(1,1) primary key,
    entity_source_id int,
    entity_name varchar(255),
    entity_property nvarchar(max),
	is_active_flag int,
    domain varchar(100),
    load_type varchar(100),
    batch_id varchar(25),
    created_time datetime DEFAULT GETDATE(),
    updated_time datetime DEFAULT GETDATE(),
    createdby varchar(100) DEFAULT SYSTEM_USER,
    updatedby varchar(100) DEFAULT SYSTEM_USER,
    foreign key (entity_source_id) references entity_source(entity_source_id)
);

create table  fwk_meta.module_audit (
    module_audit_id int identity(1,1) primary key,
    module_id int,
    data_layer varchar(100),
    status varchar(50),
    start_time datetime,
    end_time datetime,
    total_time int,
    module_metrics nvarchar(max),
    created_time datetime DEFAULT GETDATE(),
    updated_time datetime DEFAULT GETDATE(),
    createdby varchar(100) DEFAULT SYSTEM_USER,
    updatedby varchar(100) DEFAULT SYSTEM_USER,
    foreign key (module_id) references module(module_id)
);

create table  fwk_meta.task_audit(
    task_audit_id int identity(1,1) primary key,
    module_audit_id int,
    data_layer varchar(100),
    task_id int,
    app_id int,
    status varchar(100),
    start_time datetime,
    end_time datetime,
    total_time int,
    task_metrics nvarchar(max),
    created_time datetime DEFAULT GETDATE(),
    updated_time datetime DEFAULT GETDATE(),
    createdby varchar(100) DEFAULT SYSTEM_USER,
    updatedby varchar(100) DEFAULT SYSTEM_USER,
    foreign key (module_audit_id) references module_audit(module_audit_id),
    foreign key (task_id) references task(task_id),
    foreign key (app_id) references app(app_id)
);


create table  fwk_meta.transformation(
	transformation_id int identity(1,1) primary key,
	entity_id int NOT NULL,
	table_transformation nvarchar(max) NULL,
	custom_expression nvarchar(max) NULL,
	is_active_flag int,
	created_time datetime DEFAULT GETDATE(),
	updated_time datetime DEFAULT GETDATE(),
	createdby varchar(100) DEFAULT SYSTEM_USER,
	updatedby varchar(100) DEFAULT SYSTEM_USER,
	foreign key (entity_id) references entity(entity_id)
)









