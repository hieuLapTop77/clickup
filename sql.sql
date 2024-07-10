CREATE TABLE [dbo].[3rd_misa_products](
	[STT] [varchar](255) NULL,
	[Ma_SP] [nvarchar](255) NOT NULL,
	[Ten_SP] [nvarchar](255) NOT NULL,
    [Giam_thue_theo_quy_dinh] [nvarchar](255) NOT NULL,
    [Tinh_chat] [nvarchar](255) NOT NULL,
    [Nhom_VTHH] [nvarchar](255) NULL,
    [Don_vi_tinh_chinh] [nvarchar](255) NULL,
    [So_luong_ton] [varchar](255) NULL,
    [Gia_tri_ton] [varchar](255) NULL,
    [Thoi_han_bao_hanh] [nvarchar](255) NULL,
    [So_luong_ton_toi_thieu] [varchar](255) NULL,
    [Nguon_goc] [nvarchar](255) NULL,
    [Mo_ta] [nvarchar](max) NULL,
    [Dien_giai_khi_mua] [nvarchar](255) NOT NULL,
    [Dien_giai_khi_ban] [nvarchar](255) NOT NULL,
    [Ma_kho_ngam_dinh] [nvarchar](255) NULL,
    [Kho_ngam_dinh] [nvarchar](255) NULL,
	[TK_kho] [varchar](255) NULL,
	[TK_doanh_thu] [varchar](255) NULL,
	[TK_chiet_khau] [varchar](255) NULL,
	[TK giam_gia] [varchar](255) NULL,
	[TK_tra _lai] [varchar](255) NULL,
	[TK_chi_phi] [varchar](255) NULL,
    [Ty_le_CK_khi_mua_hang] [varchar](255) NULL,
    [Don_gia_mua_co_dinh] [varchar](255) NULL,
    [Don_gia_mua_gan_nhat] [varchar](255) NULL,
	[Don_gia_ban_1] [varchar](255) NULL,
	[Don_gia_ban_2] [varchar](255) NULL,
	[Don_gia_ban_3] [varchar](255) NULL,
    [Don_gia_ban_co_dinh] [varchar](255) NULL,
    [La_don_gia_sau_thue] [nvarchar](255) NULL,
    [Thue_suat_GTGT] [nvarchar](255) NULL,
    [Phan_tram_thue_suat_khac] [nvarchar](255) NULL,
    [Thue_suat_thue_NK] [nvarchar](255) NULL,
    [Thue_suat_thue_XK] [nvarchar](255) NULL,
    [Nhom_HHDV_chiu_thue_TTDB] [nvarchar](255) NULL,
    [Trang_thai] [nvarchar](255) NULL,
    [Chiet_khau_So_luong_tu] [varchar](255) NULL,
    [Chiet_khau_So_luong_den] [varchar](255) NULL,
    [Chiet_khau_Phan_tram_chiet_khau] [varchar](255) NULL,
    [Chiet_khau_So_tien_chiet_khau] [varchar](255) NULL,
    [Don_vi_chuyen_doi_Don_vi_chuyen_doi] [nvarchar](255) NULL,
    [Don_vi_chuyen_doi_Ty_le_chuyen_doi] [nvarchar](255) NULL,
    [Don_vi_chuyen_doi_Phep_tinh] [nvarchar](255) NULL,
    [Don_vi_chuyen_doi_Mo_ta] [nvarchar](255) NULL,
    [Don_vi_chuyen_doi_Don_gia_ban_1] [nvarchar](255) NULL,
    [Don_vi_chuyen_doi_Don_gia_ban_2] [nvarchar](255) NULL,
    [Don_vi_chuyen_doi_Don_gia_ban_3] [nvarchar](255) NULL,
    [Don_vi_chuyen_doi_Don_gia_co_dinh] [nvarchar](255) NULL,
    [Dinh_muc_nguyen_vat_lieu_Ma_nguyen_vat_lieu] [nvarchar](255) NULL,
    [Dinh_muc_nguyen_vat_lieu_Ten_nguyen_vat_lieu] [nvarchar](255) NULL,
    [Dinh_muc_nguyen_vat_lieu_Don_vi_tinh] [nvarchar](255) NULL,
    [Dinh_muc_nguyen_vat_lieu_So_luong] [nvarchar](255) NULL,
    [Dinh_muc_nguyen_vat_lieu_Khoan_muc_CP] [varchar](255) NULL,
    [Ma_quy_cach_Ten_quy_cach] [nvarchar](255) NULL,
    [Ma_quy_cach_Cho_phep_trung] [varchar](255) NULL,
    [dtm_creation_date] [datetime] NULL
);

CREATE TABLE [dbo].[3rd_misa_group_products](
	[ma_hang] [varchar](50) NULL,
	[ten_hang] [nvarchar](500) NULL,
	[nhan_hang] [nvarchar](200) NULL,
	[nhom_nhan_hang] [nvarchar](250) NULL,
	[dtm_creation_date] [datetime] NULL
);
CREATE TABLE [dbo].[3rd_misa_sales_details](
	[sales_details_id] [int] IDENTITY(1,1) NOT NULL,
	[NgayHachToan] [varchar](40) NULL,
	[NgayChungTu] [varchar](40) NULL,
	[SoChungTu] [varchar](200) NULL,
	[DienGiaiChung] [nvarchar](max) NULL,
	[customers_code] [nvarchar](50) NULL,
	[MaHang] [nvarchar](50) NULL,
	[DVT] [nvarchar](50) NULL,
	[SoLuongBan] [varchar](50) NULL,
	[SLBanKhuyenMai] [varchar](50) NULL,
	[TongSoLuongBan] [varchar](50) NULL,
	[DonGia] [varchar](50) NULL,
	[DoanhSoBan] [varchar](50) NULL,
	[ChietKhau] [varchar](50) NULL,
	[TongSoLuongTraLai] [varchar](50) NULL,
	[GiaTriTraLai] [varchar](50) NULL,
	[ThueGTGT] [varchar](50) NULL,
	[TongThanhToanNT] [varchar](50) NULL,
	[GiaVon] [varchar](50) NULL,
	[MaNhanVienBanHang] [nvarchar](50) NULL,
	[TenNhanVienBanHang] [nvarchar](255) NULL,
	[MaKho] [nvarchar](255) NULL,
	[TenKho] [nvarchar](255) NULL,
	[dtm_create_time] [datetime] NULL
);

CREATE TABLE [dbo].[3rd_misa_khachhang](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[MaKhachHang] [nvarchar](50) NOT NULL,
	[TenKhachHang] [nvarchar](max) NOT NULL,
	[DiaChi] [nvarchar](max) NULL,
	[CongNo] [decimal](18, 2) NULL,
	[MaSoThue] [nvarchar](200) NULL,
	[DienThoai] [nvarchar](200) NULL,
	[DTDiDongNLH] [nvarchar](200) NULL,
	[LaDoiTuongNoiBo] [nvarchar](200) NULL,

)
;

---------------------------------------------------------------

CREATE view [dbo].[jotform_view] as 
 select a.*, 
    b.name name_b,
    b.orderindex orderindex_b, 
    c.name name_c, 
    c.orderindex orderindex_c 
    --,d.name as name_d, d.orderindex as orderindex_d
from [dbo].[3rd_jotform_form_submissions] a
inner join (
    select JSON_VALUE(value, '$.id') id
        ,JSON_VALUE(value, '$.name') name
        ,JSON_VALUE(value, '$.orderindex') orderindex	
    from [dbo].[3rd_clickup_custom_fields] a
    cross apply openjson(a.type_config, '$.options') as value
    where id in ('20bd1409-47ad-41cd-b134-31c8cfb9cfae')
) 
b on a.loai_khach_hang = b.name
inner join (
    select  JSON_VALUE(value, '$.id') id
        ,JSON_VALUE(value, '$.name') name
        ,JSON_VALUE(value, '$.orderindex') orderindex
    from [dbo].[3rd_clickup_custom_fields]
    cross apply openjson(type_config, '$.options') as value
    where id = '04a66008-9468-46f6-abee-b5fb408983e0'
)c on a.loai_don_hang = c.name
where a.created_at >= DATEADD(hour, -24, GETDATE()) and a.status <> 'DELETED'
--a.dtm_creation_date >= DATEADD(hour, -24, GETDATE())
-------------------------------------------------------------------------------------


CREATE VIEW [dbo].[vw_Clickup_Tasks] AS

with tmp as ( --58
SELECT id,name,status,custom_fields,date_created
FROM [db_test].[dbo].[3rd_clickup_tasks]
where  JSON_VALUE(list, '$.id')  in (901802184458,901802268429,901802268438,901802276882,901802290640,901802287142,901802287144,901802300868,901802301544)  --đây là danh sách task cần lấy
--order by date_created
),
tmp_value as (
select jd.id,name,jd.status,jd.custom_fields,jd.date_created,
JSON_VALUE(value, '$.id') AS id_custom_fields,
JSON_VALUE(value, '$.name') AS name_custom_fields,
JSON_VALUE(value, '$.value') AS value_custom_fields,
JSON_VALUE(value, '$.type') AS type_custom_fields,
value value_list
from tmp jd
CROSS APPLY OPENJSON(jd.custom_fields) AS value
--where id='86epm7chx'
),
tmp_value_list as (
select id,name_drop_down,name_value,orderindex_value from [3rd_misa_drop_down_value]
)
,tmp_base as
(
select a.id,a.name,JSON_VALUE(a.status, '$.status') AS status,a.date_created,
a.name_custom_fields,
case when type_custom_fields='drop_down' then b.name_value else a.value_custom_fields end value_custom_fields
from tmp_value a
left join tmp_value_list b on a.value_custom_fields=b.orderindex_value and a.id_custom_fields=b.id and type_custom_fields='drop_down' 
)
SELECT 
    id,name,status,date_created,
	[Tên sản phẩm],
	[Brand NCC],
	[Account],
	[Giá bán],
	[Số lượng],
	[Công thức Giá trị dự kiến],
	[Giá trị dự kiến],
    [Nhóm ngành hàng],
    [Hàng bán/Khuyến Mãi],
    [Date kế hoạch]
FROM
(
    SELECT id, name,status, DATEADD(SECOND, cast(date_created as bigint) / 1000, '1970-01-01 00:00:00') AS date_created,name_custom_fields, value_custom_fields
    FROM tmp_base
) AS SourceTable
PIVOT
(
    MAX(value_custom_fields)
    FOR name_custom_fields IN (
        [Nhóm ngành hàng], 
        [Hàng bán/Khuyến Mãi], 
        [Account], 
        [Date kế hoạch], 
        [Brand NCC], 
        [Số lượng], 
        [Giá bán], 
        [Tên sản phẩm], 
        [Giá trị dự kiến], 
        [Công thức Giá trị dự kiến]
    )
) AS PivotTable
--order by date_created;

-------------------------------------------------------------------------

create view [dbo].[data_thuc] as 
select a.NgayChungTu,sum(cast(a.TongThanhToanNT as bigint)) TongThanhToanNT
from(
	select a.[Tên] as tenhanghoa
		   ,cast(b.NgayChungTu as date) NgayChungTu
		   ,b.SoLuongBan
		   ,b.TongSoLuongBan
		   ,b.DoanhSoBan
		   ,b.TongThanhToanNT
		   ,b.GiaVon
		   ,b.TenNhanVienBanHang
		   ,b.TenKho
		   ,c.TenKhachHang
	from [3rd_misa_products] a
	inner join [dbo].[vw_misa_sales_details] b 
	on a.[Ma_SP] = b.[MaHang]
	inner join [dbo].[3rd_misa_khachhang] c
	on c.MaKhachHang = b.customers_code
	) a
group by NgayChungTu
----------------------------------------------------------------

create view [dbo].[data_dukien] as

-- dữ liệu dự kiến
select cast(date_ke_hoach as date) date_ke_hoach, sum(cast([Giá trị dự kiến] as bigint)) doanhthudukien
from 
	(	select *, DATEADD(SECOND, cast([Date kế hoạch] as bigint) / 1000, '1970-01-01 00:00:00') AS date_ke_hoach
		from [dbo].[3rd_clickup_view]	) a
group by cast(date_ke_hoach as date)
GO



