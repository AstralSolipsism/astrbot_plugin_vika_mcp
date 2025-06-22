import json
import traceback
import os
import time
import asyncio
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import pytz

from astrbot.core.platform.astr_message_event import AstrMessageEvent
from astrbot.api.event import filter
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig


try:
    from astral_vika import Vika
except ImportError:
    logger.error("维格表MCP插件启动失败：未找到 astral_vika 库，请检查路径或安装")
    Vika = None


@register("vika_mcp_plugin", "AstralSolipsism", "智能维格表MCP插件，支持自动发现数据表，大模型自动调用各种操作功能", "0.9")
class VikaMcpPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.vika_client = None
        self.plugin_config = {}
        self.sync_lock = asyncio.Lock()
        
        # 从配置中读取并发数，默认值为5
        max_concurrent = self.plugin_config.get('max_concurrent_vika_requests', 5)
        self.vika_semaphore = asyncio.Semaphore(max_concurrent)
        
        # 智能数据表管理
        self.datasheet_mapping = {}  # 自动发现的数据表映射 {name: id}
        self.datasheet_views_mapping = {} # { "datasheet_id_1": { "view_name_1": "view_id_1", ... }, ... }
        self.spaces_list = []  # 空间站列表
        self.default_space_id = None
        self.cache_file_path = os.path.join(os.getcwd(), "data", "vika_cache.json")
        self.field_meta_cache = {}  # 新增：字段元数据缓存 {datasheet_id: {field_name: field_meta}}
        self.cache_timestamp = None
        self.is_synced = False
        
        # 加载插件配置
        self._load_config()
        
        # 初始化维格表客户端
        self._init_vika_client()
        
        # 加载缓存
        self._load_cache()

    def _load_config(self):
        """加载插件配置"""
        try:
            plugin_name = "vika_mcp_plugin"
            self.plugin_config = self.config or {}
            self.default_space_id = self.plugin_config.get('default_space_id', '')
            logger.info(f"维格表MCP插件配置加载完成: {list(self.plugin_config.keys())}")
        except Exception as e:
            logger.error(f"加载维格表MCP插件配置失败: {e}")
            self.plugin_config = {}

    def _init_vika_client(self):
        """初始化维格表客户端"""
        if not Vika:
            logger.error("维格表MCP插件初始化失败：vika 库未安装")
            return
            
        api_token = self.plugin_config.get('vika_api_token', '')
        vika_host = self.plugin_config.get('vika_host', 'https://api.vika.cn')
        
        if not api_token:
            logger.warning("维格表 API Token 未配置")
            return
            
        try:
            # 初始化客户端，支持自定义host
            self.vika_client = Vika(
                api_token,
                api_base=vika_host,
                status_callback=self._send_status_feedback
            )
            logger.info(f"维格表客户端初始化成功，服务器: {vika_host}")
        except Exception as e:
            logger.error(f"维格表客户端初始化失败: {e}")

    def _load_cache(self):
        """加载缓存的数据表信息"""
        try:
            if os.path.exists(self.cache_file_path):
                with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    
                self.datasheet_mapping = cache_data.get('datasheet_mapping', {})
                self.datasheet_views_mapping = cache_data.get('datasheet_views_mapping', {})
                self.spaces_list = cache_data.get('spaces_list', [])
                self.field_meta_cache = cache_data.get('field_meta_cache', {})
                self.cache_timestamp = cache_data.get('timestamp', 0)
                
                # 检查缓存是否过期
                cache_duration = self.plugin_config.get('cache_duration_hours', 24)
                if time.time() - self.cache_timestamp < cache_duration * 3600:
                    self.is_synced = True
                    logger.info(f"从缓存加载了 {len(self.datasheet_mapping)} 个数据表")
                else:
                    logger.info("缓存已过期，需要重新同步")
            else:
                logger.info("未找到缓存文件，需要初始同步")
        except Exception as e:
            logger.error(f"加载缓存失败: {e}")

    def _save_cache(self):
        """保存缓存到文件"""
        try:
            os.makedirs(os.path.dirname(self.cache_file_path), exist_ok=True)
            cache_data = {
                'datasheet_mapping': self.datasheet_mapping,
                'datasheet_views_mapping': self.datasheet_views_mapping,
                'spaces_list': self.spaces_list,
                'field_meta_cache': self.field_meta_cache,
                'timestamp': time.time()
            }
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            logger.info("缓存已保存")
        except Exception as e:
            logger.error(f"保存缓存失败: {e}")

    def _get_datasheet_id(self, datasheet_name: str) -> str:
        """智能获取数据表ID"""
        # 1. 首先检查自动发现的映射
        if datasheet_name in self.datasheet_mapping:
            datasheet_id = self.datasheet_mapping[datasheet_name]
            return datasheet_id
            
        # 2. 检查自定义别名
        custom_aliases = self.plugin_config.get('custom_aliases', {})
        if datasheet_name in custom_aliases:
            real_name = custom_aliases[datasheet_name]
            if real_name in self.datasheet_mapping:
                datasheet_id = self.datasheet_mapping[real_name]
                return datasheet_id
                
        # 3. 检查手动配置的映射（向后兼容）
        manual_mapping = self.plugin_config.get('datasheet_mapping', {})
        if datasheet_name in manual_mapping:
            datasheet_id = manual_mapping[datasheet_name]
            return datasheet_id
            
        # 4. 如果是直接的datasheet ID格式，直接返回
        if datasheet_name.startswith('dst'):
            return datasheet_name
            
        # 5. 模糊匹配（部分匹配）
        for name, ds_id in self.datasheet_mapping.items():
            if datasheet_name.lower() in name.lower() or name.lower() in datasheet_name.lower():
                return ds_id
                
        # 6. 都找不到，返回原名称
        logger.warning(f"未能通过任何已知方式解析数据表名称 '{datasheet_name}'。将按原样使用。")
        return datasheet_name

    def _get_datasheet(self, datasheet_name: str):
        """获取数据表对象"""
        if not self.vika_client:
            raise ValueError("维格表客户端未初始化，请检查API Token和服务器配置")
        
        datasheet_id = self._get_datasheet_id(datasheet_name)
        if not datasheet_id:
            raise ValueError(f"未找到数据表: {datasheet_name}")
        
        return self.vika_client.datasheet(datasheet_id)
        

    async def _fetch_datasheet_views(self, datasheet_id: str) -> Dict[str, str]:
        """获取单个数据表的所有视图并缓存"""
        try:
            views = await self.vika_client.datasheet(datasheet_id).views.aall()
            return {view.name: view.id for view in views}
        except Exception as e:
            logger.error(f"获取数据表 {datasheet_id} 的视图失败: {e}")
            return {}

    async def _fetch_datasheet_views_with_limit(self, datasheet_id: str) -> Dict[str, str]:
        """使用信号量限制并发地获取单个数据表的所有视图"""
        async with self.vika_semaphore:
            return await self._fetch_datasheet_views(datasheet_id)

    async def _auto_sync_if_needed(self):
        """如果需要且配置允许，自动同步数据表"""
        if (not self.is_synced and
            self.plugin_config.get('auto_sync_on_startup', True) and
            self.vika_client):
            async with self.sync_lock:
                if self.is_synced:  # 在锁内进行双重检查
                    return
                # 现在让调用者处理异常
                await self._perform_sync()

    async def _perform_sync(self):
        """执行数据表同步"""
        if not self.vika_client:
            raise ValueError("维格表客户端未初始化")
            
        # 获取空间站列表
        spaces = await self.vika_client.spaces.alist()
        
        if not spaces:
            raise ValueError("未找到任何空间站，请检查API Token权限")
            
        self.spaces_list = [{'id': space['id'], 'name': space['name']} for space in spaces]
        
        # 确定要同步的空间站
        target_space = None
        if self.default_space_id:
            target_space = next((s for s in spaces if s['id'] == self.default_space_id), None)
        
        if not target_space:
            target_space = spaces[0]  # 使用第一个空间站
            
        logger.info(f"开始同步空间站: {target_space['name']} (ID: {target_space['id']})")
        
        # 使用 v2 API 直接搜索所有数据表节点，优化同步效率
        logger.info(f"高效同步空间站 [{target_space['name']}] 中的所有数据表...")
        all_datasheet_nodes = await self.vika_client.space(target_space['id']).nodes.asearch(node_type='Datasheet')
        
        # 增强日志与验证
        logger.debug(f"从维格表API收到的原始节点数据: {all_datasheet_nodes}")
        if not all_datasheet_nodes:
            logger.warning(f"在空间站 '{target_space['name']}' (ID: {target_space['id']}) 中未发现任何数据表。请检查API Token权限或空间站内容。")

        # 验证节点结构并构建映射
        new_datasheet_map = {}
        for node in all_datasheet_nodes:
            if hasattr(node, 'name') and hasattr(node, 'id'):
                new_datasheet_map[node.name] = node.id
            else:
                logger.error(f"发现结构异常的节点对象，缺少'name'或'id'属性: {node}")
            
        logger.info(f"同步完成。发现的维格表映射: {new_datasheet_map}")
        self.datasheet_mapping.update(new_datasheet_map)
        
        # 记录同步信息
        if new_datasheet_map:
            synced_table_names = list(new_datasheet_map.keys())
            logger.info(f"成功同步 {len(synced_table_names)} 个维格表: {synced_table_names}")

        # 并发获取所有数据表的视图 (使用速率限制)
        logger.info("开始并发获取所有数据表的视图（带速率限制）...")
        tasks = [self._fetch_datasheet_views_with_limit(ds_id) for ds_id in self.datasheet_mapping.values()]
        view_results = await asyncio.gather(*tasks)
        
        # 更新视图映射缓存
        datasheet_ids = list(self.datasheet_mapping.values())
        for i, views_map in enumerate(view_results):
            datasheet_id = datasheet_ids[i]
            if views_map:
                self.datasheet_views_mapping[datasheet_id] = views_map
        logger.info(f"视图信息同步完成，共处理 {len(self.datasheet_views_mapping)} 个数据表的视图。")
            
        self.is_synced = True
        self.cache_timestamp = time.time()
        
        # 保存到缓存
        self._save_cache()
        
        return len(new_datasheet_map)

    def _format_records_to_json(self, records: List[Any]) -> str:
        """将记录列表格式化为JSON字符串，以便AI理解。"""
        if not records:
            return json.dumps([])

        record_list = []
        for record in records:
            # 假设 record 对象有 .id 和 .fields 属性
            record_dict = {
                "record_id": getattr(record, 'id', ''),
                "fields": getattr(record, 'fields', {})
            }
            record_list.append(record_dict)
        
        return json.dumps(record_list, ensure_ascii=False, indent=2)

    def _format_records_for_display(self, records: List[Dict[str, Any]], limit: int = None) -> str:
        """格式化记录为可读的文本格式"""
        if not records:
            return "没有找到任何记录"

        max_display = limit or self.plugin_config.get('max_records_display', 20)
        display_records = records[:max_display]
        
        # 构建表格显示
        result = f"找到 {len(records)} 条记录"
        if len(records) > max_display:
            result += f"（显示前 {max_display} 条）"
        result += ":\\n\\n"
        
        if display_records:
            # 获取所有字段名
            all_fields = set()
            for record in display_records:
                record_fields = getattr(record, 'fields', None)
                if record_fields:
                    all_fields.update(record_fields.keys())
            
            field_list = list(all_fields)
            
            # 表头
            result += "| " + " | ".join(field_list) + " |\\n"
            result += "|" + "---|" * len(field_list) + "\\n"
            
            # 数据行
            for record in display_records:
                fields = getattr(record, 'fields', {})
                row_data = []
                for field in field_list:
                    value = fields.get(field, '')
                    # 处理特殊字符和长文本
                    if isinstance(value, str):
                        value = value.replace('|', '\\|').replace('\\n', ' ')
                        if len(value) > 30:
                            value = value[:30] + "..."
                    elif value is None:
                        value = ""
                    row_data.append(str(value))
                result += "| " + " | ".join(row_data) + " |\\n"
            
        return result

    async def _get_field_meta(self, datasheet) -> Dict[str, Any]:
        """获取并缓存数据表的字段元数据"""
        if datasheet.dst_id in self.field_meta_cache:
            return self.field_meta_cache[datasheet.dst_id]
        
        fields = await datasheet.fields.aall()
        meta = {field.name: field for field in fields}
        self.field_meta_cache[datasheet.dst_id] = meta
        self._save_cache() # 保存到缓存
        return meta

    async def _resolve_field_values(self, datasheet, record_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        智能解析和转换字段值。
        将用户友好的输入（如姓名、选项文本、关联记录标题）转换为Vika API所需的ID格式。
        """
        field_meta = await self._get_field_meta(datasheet)
        resolved_fields = {}

        for field_name, user_value in record_data.items():
            if field_name not in field_meta:
                # 如果字段不存在，直接使用用户提供的值
                resolved_fields[field_name] = user_value
                continue

            field_info = field_meta[field_name]
            field_type = field_info.type

            # --- 成员字段解析 ---
            if field_type in ["Member", "CreatedBy", "LastModifiedBy"] and isinstance(user_value, str):
                space_id = datasheet.space_id
                members = await self.vika_client.space(space_id).nodes.asearch(node_type='Member')
                member_map = {member.name: member.id for member in members}
                if user_value in member_map:
                    resolved_fields[field_name] = [member_map[user_value]]
                else:
                    logger.warning(f"在空间站中未找到成员 '{user_value}'，将按原样发送")
                    resolved_fields[field_name] = user_value
                continue

            # --- 单选/多选字段解析 ---
            elif field_type in ["SingleSelect", "MultiSelect"]:
                options_map = {opt['name']: opt['id'] for opt in field_info.property['options']}
                if isinstance(user_value, list): # 多选
                    resolved_ids = [options_map[val] for val in user_value if val in options_map]
                    resolved_fields[field_name] = resolved_ids
                elif isinstance(user_value, str) and user_value in options_map: # 单选
                    resolved_fields[field_name] = options_map[user_value]
                else:
                    logger.warning(f"在字段 '{field_name}' 的选项中未找到 '{user_value}'")
                    resolved_fields[field_name] = user_value
                continue

            # --- 关联记录字段解析 ---
            elif field_type in ["OneWayLink", "TwoWayLink"] and isinstance(user_value, (str, list)):
                foreign_dst_id = field_info.property.get('foreignDatasheetId')
                if not foreign_dst_id:
                    resolved_fields[field_name] = user_value
                    continue
                
                foreign_datasheet = self.vika_client.datasheet(foreign_dst_id)
                
                # 确保传入的是列表
                search_titles = user_value if isinstance(user_value, list) else [user_value]
                
                # 使用 in 查询批量获取记录
                # 注意：这里假设关联记录的标题是主字段，且可以直接查询
                # Vika API目前不直接支持按标题批量查询，这里简化为循环查询，实际应用中可优化
                record_ids = []
                for title in search_titles:
                    try:
                        # 假设主字段名为'标题'或'名称'，这需要根据实际情况调整
                        # 更稳健的做法是获取关联表的第一个文本字段作为查询字段
                        records = await foreign_datasheet.records.filter(f"{{主字段}}='{title}'").aall()
                        if records:
                            record_ids.append(records[0].id)
                        else:
                            logger.warning(f"在关联表 '{foreign_dst_id}' 中未找到标题为 '{title}' 的记录")
                    except Exception as e:
                        logger.error(f"查询关联记录失败: {e}")

                if record_ids:
                    resolved_fields[field_name] = record_ids
                else:
                    resolved_fields[field_name] = user_value
                continue

            # --- 日期时间解析 (可扩展) ---
            elif field_type == "DateTime" and isinstance(user_value, str):
                try:
                    # 尝试多种格式
                    dt = datetime.fromisoformat(user_value.replace('Z', '+00:00'))
                    resolved_fields[field_name] = int(dt.timestamp() * 1000)
                except ValueError:
                    logger.warning(f"日期格式 '{user_value}' 解析失败，将按原样发送")
                    resolved_fields[field_name] = user_value
                continue

            # --- 其他字段直接赋值 ---
            else:
                resolved_fields[field_name] = user_value
        
        return resolved_fields

    async def _build_formula_from_filter(self, datasheet, filter_by: Dict[str, Any]) -> str:
        """
        从 filter_by 字典构建 Vika 查询公式。
        """
        if not filter_by:
            return ""

        field_meta = await self._get_field_meta(datasheet)
        formula_parts = []

        for field_name, value in filter_by.items():
            if field_name not in field_meta:
                # 如果字段不存在，尝试进行简单的字符串匹配
                formula_parts.append(f"{{{field_name}}}='{value}'")
                continue

            field_info = field_meta[field_name]
            field_type = field_info.type

            # --- 单选/多选字段解析 ---
            if field_type == "SingleSelect":
                options_map = {opt['name']: opt['id'] for opt in field_info.property['options']}
                if isinstance(value, str) and value in options_map:
                    formula_parts.append(f"{{{field_name}}}='{options_map[value]}'")
                else:
                    logger.warning(f"在字段 '{field_name}' 的选项中未找到 '{value}'，将忽略此筛选条件")
            
            elif field_type == "MultiSelect":
                options_map = {opt['name']: opt['id'] for opt in field_info.property['options']}
                values_to_check = value if isinstance(value, list) else [value]
                ids = [options_map.get(v) for v in values_to_check if v in options_map]
                if ids:
                    # 对于多选，检查每个ID是否存在
                    or_parts = [f"FIND('{i}', ARRAYJOIN({{{field_name}}}))" for i in ids]
                    formula_parts.append(f"OR({','.join(or_parts)})")
                else:
                    logger.warning(f"在字段 '{field_name}' 的选项中未找到任何有效值，将忽略此筛选条件")

            # --- 成员字段解析 ---
            elif field_type == "Member":
                space_id = datasheet.space_id
                members = await self.vika_client.space(space_id).nodes.asearch(node_type='Member')
                member_map = {member.name: member.id for member in members}
                if value in member_map:
                    formula_parts.append(f"{{{field_name}}}='{member_map[value]}'")
                else:
                    logger.warning(f"在空间站中未找到成员 '{value}'，将忽略此筛选条件")

            # --- 关联记录字段解析 ---
            elif field_type in ["OneWayLink", "TwoWayLink"]:
                 # 注意：按关联记录的文本标题进行过滤通常很复杂且效率低下。
                 # Vika API 本身不支持直接这样做。这里我们假设用户提供了记录ID。
                 # 如果用户提供的是文本，我们需要先搜索ID，这会增加复杂性。
                 # 目前，我们只支持按记录ID过滤。
                if isinstance(value, str) and value.startswith('rec'):
                    formula_parts.append(f"{{{field_name}}}='{value}'")
                else:
                    logger.warning(f"关联字段 '{field_name}' 的筛选目前只支持记录ID (rec...)，已忽略值 '{value}'")
            
            # --- 其他字段直接进行等值比较 ---
            else:
                if isinstance(value, str):
                    formula_parts.append(f"{{{field_name}}}='{value}'")
                elif isinstance(value, (int, float)):
                    formula_parts.append(f"{{{field_name}}}={value}")
                else:
                    logger.warning(f"不支持对字段 '{field_name}' 的值 '{value}' (类型: {type(value)}) 进行筛选")

        if not formula_parts:
            return ""
        
        return f"AND({','.join(formula_parts)})"

    @filter.llm_tool(name="sync_vika_datasheets")
    async def sync_vika_datasheets(self, event: AstrMessageEvent) -> str:
        """同步并缓存维格空间站中的所有数据表，让您可以通过名称直接操作它们。
        """
        if not self.vika_client:
            return "❌ 错误：维格表客户端未初始化，请检查API Token配置"
            
        try:
            discovered_count = await self._perform_sync()
            
            result = f"✅ 同步完成！共发现 {discovered_count} 个数据表。\n\n"
            result += "📋 **已发现的数据表**：\n"
            
            for name, ds_id in list(self.datasheet_mapping.items())[:10]:  # 只显示前10个
                result += f"• {name} (`{ds_id}`)\n"
                
            if len(self.datasheet_mapping) > 10:
                result += f"• ... 还有 {len(self.datasheet_mapping) - 10} 个数据表\n"
                
            result += "\n💡 现在您可以直接通过数据表名称来操作它们了！"
            
            return result
            
        except Exception as e:
            error_msg = f"❌ 同步维格表失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="list_vika_spaces")
    async def list_vika_spaces(self, event: AstrMessageEvent) -> str:
        """列出您在维格表平台中创建或有权访问的所有空间站（即表格的组织容器）。
        """
        if not self.vika_client:
            return "❌ 错误：维格表客户端未初始化，请检查API Token配置"
            
        try:
            spaces = await self.vika_client.spaces.alist()
            
            if not spaces:
                return "📭 未找到任何空间站，请检查您的API Token权限"
                
            result = f"🏢 **您的维格表空间站** (共 {len(spaces)} 个)：\n\n"
            
            for space in spaces:
                is_default = " 🔸 *默认*" if space['id'] == self.default_space_id else ""
                result += f"• **{space['name']}**{is_default}\n"
                result += f"  ID: `{space['id']}`\n\n"
                
            if not self.default_space_id and len(spaces) > 1:
                result += "💡 **提示**: 如果您有多个空间站，建议在配置中设置 `default_space_id` 以指定默认操作的空间站。"
                
            return result
            
        except Exception as e:
            error_msg = f"❌ 获取空间站列表失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="list_vika_datasheets")
    async def list_vika_datasheets(self, event: AstrMessageEvent, space_id: str = None, filter_keyword: str = None) -> str:
        """列出维格表中的所有具体数据表格（Datasheet），支持指定空间站和关键词过滤。

        Args:
            space_id(string): 可选，指定要列出数据表的空间站ID。如果未提供，将使用默认空间站或已同步的数据表。
            filter_keyword(string): 可选，过滤关键词，只显示包含该关键词的数据表。
        """
        if not self.vika_client:
            return "❌ 错误：维格表客户端未初始化，请检查API Token配置"

        try:
            datasheets_to_list = {}
            space_id_to_query = space_id
            
            # 如果用户没有提供 space_id，则检查并使用默认配置
            if not space_id_to_query:
                space_id_to_query = self.default_space_id

            if space_id_to_query and isinstance(space_id_to_query, str):
                try:
                    # 始终使用高效的搜索API
                    logger.info(f"使用高效搜索API在空间站 [{space_id_to_query}] 中查找所有数据表...")
                    all_datasheet_nodes = await self.vika_client.space(space_id_to_query).nodes.asearch(node_type='Datasheet')
                    datasheets_to_list = {node.name: node.id for node in all_datasheet_nodes}
                except Exception as e:
                    logger.error(f"无法获取空间站 '{space_id_to_query}' 中的数据表: {e}\n{traceback.format_exc()}")
                    return f"❌ 无法获取空间站 '{space_id_to_query}' 中的数据表，请检查空间站ID和权限: {str(e)}"
            else:
                # 如果没有指定space_id，则使用已同步的数据表
                await self._auto_sync_if_needed()
                if not self.is_synced:
                    return "⚠️ 数据表列表尚未同步，请先运行数据表同步功能。"
                datasheets_to_list = self.datasheet_mapping

            if not datasheets_to_list:
                return "📭 未发现任何数据表，请检查空间站中是否有数据表，或重新同步。"

            # 应用过滤
            filtered_tables = {}
            if filter_keyword:
                for name, ds_id in datasheets_to_list.items():
                    if filter_keyword.lower() in name.lower():
                        filtered_tables[name] = ds_id
            else:
                filtered_tables = datasheets_to_list

            if not filtered_tables:
                return f"🔍 未找到包含关键词 '{filter_keyword}' 的数据表。"

            result = f"📊 **数据表列表**"
            if space_id_to_query:
                result += f" (来自空间站: `{space_id_to_query}`)"
            if filter_keyword:
                result += f" (包含 '{filter_keyword}')"
            result += f" (共 {len(filtered_tables)} 个)：\n\n"

            for name, ds_id in filtered_tables.items():
                result += f"• **{name}**\n"
                result += f"  ID: `{ds_id}`\n\n"

            # 显示自定义别名提示
            custom_aliases = self.plugin_config.get('custom_aliases', {})
            if custom_aliases:
                result += "🏷️ **自定义别名**：\n"
                for alias, real_name in custom_aliases.items():
                    if real_name in self.datasheet_mapping: # 仅显示已同步的别名
                        result += f"• `{alias}` → {real_name}\n"
                result += "\n"

            result += "💡 **提示**: 您可以直接使用数据表名称进行操作，系统会自动识别。"
            
            return result

        except Exception as e:
            error_msg = f"❌ 查询表格失败：{str(e)}。请检查您的API Token权限或空间站ID是否正确，并查看后台日志获取详细错误信息。"
            logger.error(f"获取数据表列表失败: {e}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="create_vika_datasheet")
    async def create_vika_datasheet(self, event: AstrMessageEvent, datasheet_name: str, fields: List[Dict[str, Any]]) -> str:
        """在维格表空间站中创建一个新的数据表，支持定义复杂的字段属性。

        Args:
            datasheet_name(string): 要创建的数据表的名称。
            fields(array): 字段列表。每个字段是一个字典，必须包含 "name" 和 "type"。
                         对于复杂字段，可以额外提供 "property" 字典。
                         - 单选/多选: `{"name": "状态", "type": "SingleSelect", "property": {"options": [{"name": "待处理"}, {"name": "进行中"}]}}`
                         - 智能公式: `{"name": "总价", "type": "Formula", "property": {"expression": "{数量} * {单价}"}}`
                         - 关联他表: `{"name": "关联项目", "type": "OneWayLink", "property": {"foreignDatasheetId": "dst_xxx"}}`
                         - 按钮: `{"name": "触发操作", "type": "Button", "property": {"label": "点击运行", "style": "primary"}}`
        """
        if not self.vika_client:
            return "❌ 错误：维格表客户端未初始化，请检查API Token配置"
            
        try:
            # 确定目标空间站
            spaces = await self.vika_client.spaces.alist()
            
            if not spaces:
                return "❌ 未找到任何空间站，请检查API Token权限"
                
            target_space = None
            if self.default_space_id:
                target_space = next((s for s in spaces if s['id'] == self.default_space_id), None)
            
            if not target_space:
                target_space = spaces[0]
                
            # 创建数据表
            create_params = {
                'name': datasheet_name,
                'fields': fields,
                'folderId': target_space['id']
            }
            
            new_datasheet = await self.vika_client.space(target_space['id']).datasheets.acreate(**create_params)
            
            # 更新本地缓存
            self.datasheet_mapping[datasheet_name] = new_datasheet.id
            self._save_cache()
            
            result = f"✅ 数据表创建成功！\n\n"
            result += f"📊 **数据表名称**: {datasheet_name}\n"
            result += f"🆔 **数据表ID**: `{new_datasheet.id}`\n"
            result += f"🏢 **所在空间站**: {target_space['name']}\n"
            
            if fields:
                result += f"📋 **字段数量**: {len(fields)} 个\n"
                
            result += "\n💡 数据表已添加到缓存，您可以直接通过名称操作它。"
            
            return result
            
        except Exception as e:
            error_msg = f"❌ 创建数据表失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    async def _check_sync_and_suggest(self, datasheet_name: str) -> Optional[str]:
        """检查数据表是否同步，如果未找到则提供建议"""
        if not self.vika_client:
            return "❌ 维格表客户端未初始化，请检查API Token配置"

        # 确保在检查前已尝试同步
        await self._auto_sync_if_needed()
            
        # 尝试智能获取数据表ID
        datasheet_id = self._get_datasheet_id(datasheet_name)
        
        # 如果没有找到匹配的数据表，提供建议
        if datasheet_id == datasheet_name and not datasheet_name.startswith('dst'):
            if not self.is_synced:
                msg = f"⚠️ 未找到数据表 '{datasheet_name}'，且数据表列表自动同步失败。请检查配置或手动同步。"
                return msg
            else:
                # 提供相似的数据表建议
                similar_tables = []
                for table_name in self.datasheet_mapping.keys():
                    if any(word in table_name.lower() for word in datasheet_name.lower().split()):
                        similar_tables.append(table_name)
                
                if similar_tables:
                    suggestion = f"❌ 未找到数据表 '{datasheet_name}'。您是否想要操作：\n"
                    for table in similar_tables[:3]:  # 最多显示3个建议
                        suggestion += f"• {table}\n"
                    return suggestion
                else:
                    available_tables = list(self.datasheet_mapping.keys())[:5]
                    msg = (f"❌ 未找到数据表 '{datasheet_name}'。\n"
                           f"可用的数据表：{', '.join(available_tables)}")
                    return msg
        
        return None  # 找到了数据表，无需提示

    @filter.llm_tool(name="get_vika_records")
    async def get_vika_records(
        self,
        event: AstrMessageEvent,
        datasheet_name: str,
        filter_by: Optional[Dict[str, Any]] = None,
        formula: str = None,
        view_id: str = None,
        sort: str = None,
        fields: str = None,
        max_records: str = None,
        page_size: str = None,
        page_num: str = None,
        field_key: str = None,
        cell_format: str = None
    ) -> str:
        """
        查询维格表记录。这是获取、过滤和搜索数据的唯一且功能最强大的方法。

        支持三种过滤方式（优先级从高到低）:
        1. `formula`: 使用维格表原生公式进行复杂查询，功能最强。
        2. `filter_by`: 使用简单的 "字段: 值" 字典进行等值查询，系统会自动处理复杂字段。
        3. 如需进行关键词搜索，请使用 `formula` 参数，例如 `formula="FIND('关键词', {要搜索的字段名})"`。

        Args:
            datasheet_name(string): 要查询的数据表的准确名称或别名。
            filter_by(object): 可选，一个简单的键值对字典，用于过滤记录。系统会自动处理复杂字段。
                               - `{"状态": "处理中"}` (会自动解析为单选字段的ID)
                               - `{"负责人": "张三"}` (会自动解析为成员字段的ID)
                               - `{"标签": ["重要", "紧急"]}` (会自动解析为多选字段的ID)
                               - `{"关联项目": "recAbc123"}` (关联字段目前仅支持按记录ID查询)
            formula(string): 可选，一个维格表查询公式。功能最强大，推荐使用。如果同时提供了 filter_by，此项优先。
                         - **精确匹配**: `"{标题}='张三的报告'"`
                         - **模糊搜索**: `"FIND('报告', {标题})"`
                         - **组合条件**: `"AND({状态}='已完成', {分数}>=60)"`
            view_id(string): 可选，要查询的视图ID或视图名称。如果未指定，则查询所有记录。
            sort(string): 可选，排序字段。多个字段用逗号分隔，降序在字段名前加'-'。示例: '字段A,-字段B'。
            fields(string): 可选，指定返回的字段。多个字段用逗号分隔。示例: '字段A,字段B'。
            max_records(string): 可选，指定本次请求返回的最大记录数。
            page_size(string): 可选，指定每页返回的记录数，与 page_num 配合使用。
            page_num(string): 可选，指定分页的页码，与 page_size 配合使用。
            field_key(string): 可选，指定字段的返回键，'name' (字段名) 或 'id' (字段ID)。默认为 'name'。
            cell_format(string): 可选，指定单元格值的返回格式，'json' 或 'string'。默认为 'json'。
        """
        try:
            logger.info(f"开始执行 get_vika_records, datasheet_name='{datasheet_name}', filter_by={filter_by}")
            if not self.vika_client:
                return "❌ 错误：维格表客户端未初始化，请检查API Token配置"

            # --- 参数类型转换和验证 ---
            parsed_max_records = None
            if max_records:
                try:
                    parsed_max_records = int(max_records)
                except (ValueError, TypeError):
                    return f"❌ 参数 'max_records' 的值 '{max_records}' 必须是一个有效的整数。"

            parsed_page_size = None
            if page_size:
                try:
                    parsed_page_size = int(page_size)
                except (ValueError, TypeError):
                    return f"❌ 参数 'page_size' 的值 '{page_size}' 必须是一个有效的整数。"

            parsed_page_num = None
            if page_num:
                try:
                    parsed_page_num = int(page_num)
                except (ValueError, TypeError):
                    return f"❌ 参数 'page_num' 的值 '{page_num}' 必须是一个有效的整数。"
            # --- 参数处理结束 ---

            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
            
            datasheet = self._get_datasheet(datasheet_name)
            logger.info(f"成功定位数据表: {datasheet.dst_id}")

            # --- 查询逻辑 ---
            final_formula = formula
            if not final_formula and filter_by:
                logger.info(f"正在从 filter_by 构建查询公式: {filter_by}")
                final_formula = await self._build_formula_from_filter(datasheet, filter_by)
                logger.info(f"构建的查询公式为: {final_formula}")
            
            if formula and filter_by:
                logger.warning(f"同时提供了 'formula' 和 'filter_by'，将优先使用 'formula': {formula}")

            # 初始化查询集
            qs = datasheet.records.all()

            # 解析视图ID
            final_view_id = view_id
            if view_id and not view_id.startswith('viw'):
                datasheet_id = datasheet.dst_id
                if datasheet_id in self.datasheet_views_mapping and view_id in self.datasheet_views_mapping[datasheet_id]:
                    final_view_id = self.datasheet_views_mapping[datasheet_id][view_id]
                    logger.info(f"已将视图名称 '{view_id}' 解析为 ID '{final_view_id}'")
                else:
                    logger.warning(f"未在缓存中找到名为 '{view_id}' 的视图，将按原样使用。")

            # 应用各种查询参数
            if final_formula:
                qs = qs.filter(final_formula)
                logger.info(f"使用公式进行查询: {final_formula}")
            if final_view_id:
                qs = qs.view(final_view_id)
                logger.info(f"按视图进行查询: {final_view_id}")
            if fields:
                qs = qs.fields(*[f.strip() for f in fields.split(',')])
            if sort:
                qs = qs.sort(*[s.strip() for s in sort.split(',')])
            if parsed_max_records is not None:
                qs = qs.limit(parsed_max_records)
            if parsed_page_size is not None:
                qs = qs.page_size(parsed_page_size)
            if parsed_page_num is not None:
                qs = qs.page_num(parsed_page_num)
            if field_key:
                qs = qs.field_key(field_key)
            if cell_format:
                qs = qs.cell_format(cell_format)

            # 执行查询
            records = []
            try:
                logger.info("正在尝试连接维格表并查询数据...")
                records = await qs.aall()
                logger.info(f"成功从维格表API获取到 {len(records)} 条记录。")
            except Exception as e:
                logger.error(f"查询维格表API时发生异常: {e}\n{traceback.format_exc()}")
                raise  # 重新抛出异常，由外层捕获

            # 格式化为JSON并返回
            formatted_content = self._format_records_to_json(records)
            
            logger.info(f"成功处理数据表 '{datasheet_name}' 的 {len(records)} 条记录。")
            
            return formatted_content

        except Exception as e:
            error_msg = f"❌ 查询维格表 '{datasheet_name}' 内容失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="add_vika_record")
    async def add_vika_record(
        self,
        datasheet_name: str,
        record_data: Dict[str, Any]
    ) -> str:
        """向指定的维格表中添加新记录，支持智能解析复杂字段。

        Args:
            datasheet_name(string): 要添加记录的数据表名称或别名（必需）。
            record_data(object): 记录数据，一个包含字段名和对应值的字典。
                                 - **普通文本/数字/日期**: `{"标题": "新任务", "数量": 10, "截止日期": "2024-12-31"}`
                                 - **成员字段**: `{"负责人": "张三"}` (会自动匹配姓名并转换为用户ID)
                                 - **单选字段**: `{"状态": "待处理"}` (会自动匹配选项文本)
                                 - **多选字段**: `{"标签": ["重要", "紧急"]}` (会自动匹配多个选项文本)
                                 - **关联记录**: `{"关联项目": "项目A的标题"}` 或 `{"关联项目": ["项目A", "项目B"]}` (会自动查找并关联记录ID)
                                 - **附件**: `{"附件字段": [{"token": "attxxxx"}]}` (需要先上传文件获取token)
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            if not record_data:
                return "❌ 错误：记录数据为空。"
            
            # 智能解析字段数据
            resolved_fields = await self._resolve_field_values(datasheet, record_data)
            
            # 直接执行异步操作
            result = await datasheet.records.acreate(resolved_fields)
            
            logger.info(f"成功向维格表 [{datasheet_name}] 添加了 1 条新记录: {result.id}")
            return f"✅ 成功添加记录到数据表 '{datasheet_name}'，记录ID: {result.id}"
            
        except Exception as e:
            error_msg = f"❌ 添加维格表记录失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg


    @filter.llm_tool(name="update_vika_record")
    async def update_vika_record(self, event: AstrMessageEvent, datasheet_name: str, record_id: str, record_data: Dict[str, Any]) -> str:
        """更新指定维格表中的记录，支持智能解析复杂字段。

        Args:
            datasheet_name(string): 记录所在的数据表的名称。
            record_id(string): 要更新的记录的 ID。
            record_data(object): 一个包含要更新的字段和新值的字典。输入格式与'add_vika_record'完全相同。
                                 例如: `{"状态": "已完成", "负责人": "李四"}`
                                 - **更新附件**: `{"附件字段": [{"token": "attnewtoken"}]}`
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            if not record_data:
                return "❌ 错误：更新数据为空"
            
            # 智能解析字段数据
            resolved_fields = await self._resolve_field_values(datasheet, record_data)
            
            # 直接执行异步操作
            await datasheet.records.aupdate([{'recordId': record_id, 'fields': resolved_fields}])
            
            logger.info(f"成功在维格表 [{datasheet_name}] 中更新了 1 条记录: {record_id}")
            return f"✅ 成功更新数据表 '{datasheet_name}' 中的记录 {record_id}"
            
        except Exception as e:
            error_msg = f"❌ 更新维格表记录失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="delete_vika_record")
    async def delete_vika_record(self, event: AstrMessageEvent, datasheet_name: str, record_id: str) -> str:
        """删除指定维格表中的记录。

        Args:
            datasheet_name(string): 记录所在的数据表的名称。
            record_id(string): 要删除的记录的 ID。
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            # 直接执行异步操作
            await datasheet.records.adelete(record_id)
            
            return f"✅ 成功删除数据表 '{datasheet_name}' 中的记录 {record_id}"
            
        except Exception as e:
            error_msg = f"❌ 删除维格表记录失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="get_vika_fields")
    async def get_vika_fields(
        self,
        datasheet_name: str
    ) -> str:
        """获取指定维格表的字段信息。

        Args:
            datasheet_name(string): 数据表名称或别名（必需）
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            # 获取字段信息
            fields = await datasheet.fields.aall()
            
            if not fields:
                return f"📋 数据表 '{datasheet_name}' 没有字段信息"
            
            result = f"📋 **数据表 '{datasheet_name}' 的字段信息** (共 {len(fields)} 个字段):\n\n"
            for field in fields:
                result += f"• **{field.name}** (类型: {field.type})"
                if hasattr(field, 'description') and field.description:
                    result += f" - {field.description}"
                result += "\n"
            
            result += "\n💡 您可以使用这些字段名来添加或更新记录。"
            return result
            
        except Exception as e:
            error_msg = f"❌ 获取维格表字段信息失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="get_vika_status")
    async def get_vika_status(self) -> str:
        """检查维格表插件的连接状态、配置信息和数据表同步状态。
        """
        try:
            # 确保状态检查前已尝试同步
            await self._auto_sync_if_needed()
            
            status = "🔧 **维格表MCP插件状态**\n\n"
            
            # 检查客户端状态
            if self.vika_client:
                status += "✅ **客户端连接**: 正常\n"
            else:
                status += "❌ **客户端连接**: 未建立\n"
            
            # 检查配置
            api_token = self.plugin_config.get('vika_api_token', '')
            vika_host = self.plugin_config.get('vika_host', 'https://api.vika.cn')
            
            if api_token:
                status += f"🔑 **API Token**: 已配置 ({api_token[:8]}...)\n"
            else:
                status += "🔑 **API Token**: ❌ 未配置\n"
            
            status += f"🌐 **服务器地址**: {vika_host}\n"
            
            # 空间站信息
            if self.spaces_list:
                status += f"🏢 **空间站**: 共 {len(self.spaces_list)} 个\n"
                if self.default_space_id:
                    default_space = next((s for s in self.spaces_list if s['id'] == self.default_space_id), None)
                    if default_space:
                        status += f"🔸 **默认空间站**: {default_space['name']}\n"
            else:
                status += "🏢 **空间站**: 未同步\n"
            
            # 数据表同步状态
            if self.is_synced:
                cache_age = int((time.time() - self.cache_timestamp) / 3600) if self.cache_timestamp else 0
                status += f"📊 **数据表同步**: ✅ 已同步 ({len(self.datasheet_mapping)} 个表)\n"
                status += f"🕐 **缓存时间**: {cache_age} 小时前\n"
                
                # 显示部分数据表
                if self.datasheet_mapping:
                    status += "📋 **部分数据表**:\n"
                    for i, (name, _) in enumerate(list(self.datasheet_mapping.items())[:5]):
                        status += f"   • {name}\n"
                    if len(self.datasheet_mapping) > 5:
                        status += f"   • ... 还有 {len(self.datasheet_mapping) - 5} 个\n"
            else:
                status += "📊 **数据表同步**: ❌ 未同步\n"
                status += "💡 **建议**: 运行同步功能来发现您的数据表\n"
            
            # 配置特性
            auto_sync = self.plugin_config.get('auto_sync_on_startup', True)
            cache_duration = self.plugin_config.get('cache_duration_hours', 24)
            max_records = self.plugin_config.get('max_records_display', 20)
            
            status += f"\n⚙️ **配置信息**:\n"
            status += f"🔄 **启动同步**: {'✅ 已启用' if auto_sync else '❌ 已禁用'}\n"
            status += f"💾 **缓存时长**: {cache_duration} 小时\n"
            status += f"📊 **最大显示**: {max_records} 条记录\n"
            
            # 自定义别名
            custom_aliases = self.plugin_config.get('custom_aliases', {})
            if custom_aliases:
                status += f"🏷️ **自定义别名**: {len(custom_aliases)} 个\n"
            
            # 手动映射（向后兼容）
            manual_mapping = self.plugin_config.get('datasheet_mapping', {})
            if manual_mapping:
                status += f"📝 **手动映射**: {len(manual_mapping)} 个 (向后兼容)\n"
            
            return status
            
        except Exception as e:
            error_msg = f"❌ 获取状态信息失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="list_vika_views")
    def list_vika_views(self, event: AstrMessageEvent, datasheet_name: str) -> str:
        """列出指定数据表的所有可用视图。

        Args:
            datasheet_name(string): 要查询视图的数据表的名称。
        """
        try:
            datasheet_id = self._get_datasheet_id(datasheet_name)
            if not datasheet_id or datasheet_id not in self.datasheet_views_mapping:
                return f"❌ 未找到数据表 '{datasheet_name}' 的视图信息。请先同步或检查名称。"

            views = self.datasheet_views_mapping.get(datasheet_id, {})
            if not views:
                return f"📋 数据表 '{datasheet_name}' 中没有找到任何视图。"

            result = f"📋 **数据表 '{datasheet_name}' 的视图列表** (共 {len(views)} 个):\n\n"
            for view_name, view_id in views.items():
                result += f"• **{view_name}**\n"
                result += f"  ID: `{view_id}`\n"
            
            result += "\n💡 您可以在查询时使用 `view_id` 来获取特定视图的数据。"
            return result

        except Exception as e:
            error_msg = f"❌ 获取视图列表失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg
    @filter.llm_tool(name="get_current_time")
    async def get_current_time(
        self,
        event: AstrMessageEvent,
        timezone_str: str = 'UTC',
        format_str: str = None
    ) -> str:
        """
        获取当前时间，支持指定时区和格式。

        Args:
            timezone_str(string): 可选，用于指定时区，例如 'Asia/Shanghai' 或 'UTC'。默认为 'UTC'。
            format_str(string): 可选，用于指定返回时间的格式，遵循 strftime 标准。如果未提供，则返回 ISO 8601 格式的字符串。
        """
        try:
            # 获取当前UTC时间
            now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)

            # 设置目标时区
            target_tz = pytz.timezone(timezone_str)
            now_local = now_utc.astimezone(target_tz)

            # 格式化输出
            if format_str:
                return now_local.strftime(format_str)
            else:
                return now_local.isoformat()
        except pytz.UnknownTimeZoneError:
            return f"❌ 错误：未知的时区 '{timezone_str}'"
        except Exception as e:
            error_msg = f"❌ 获取当前时间失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="upload_vika_attachment")
    async def upload_vika_attachment(
        self,
        event: AstrMessageEvent, 
        datasheet_name: str, 
        file_path: str,
        record_id: str = None,
        field_name: str = None
    ) -> str:
        """将本地文件作为附件上传到维格表，并可选择直接关联到指定记录的特定字段。

        Args:
            datasheet_name(string): 附件要上传到的数据表的名称。
            file_path(string): 要上传的本地文件的完整路径。
            record_id(string): 可选，要将附件添加到的记录的ID。
            field_name(string): 可选，要添加附件的字段名称。如果提供了 record_id，此项为必需。
        """
        try:
            if not self.vika_client:
                return "❌ 错误：维格表客户端未初始化，请检查API Token配置"

            if not os.path.exists(file_path):
                return f"❌ 错误：文件未找到 '{file_path}'"

            if record_id and not field_name:
                return "❌ 错误：当提供 record_id 时，必须同时提供 field_name。"

            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
            
            datasheet = self._get_datasheet(datasheet_name)
            logger.info(f"准备向数据表 '{datasheet_name}' (ID: {datasheet.dst_id}) 上传文件: {file_path}")

            # 步骤 1: 执行上传
            attachment = await datasheet.attachments.aupload(file_path)
            logger.info(f"文件上传成功: {attachment.name} (Token: {attachment.token})")

            # 步骤 2: 如果提供了记录ID和字段名，则直接更新记录
            if record_id and field_name:
                logger.info(f"准备将附件关联到记录 '{record_id}' 的字段 '{field_name}'")
                update_data = {
                    'recordId': record_id,
                    'fields': {
                        field_name: [
                            {'token': attachment.token}
                        ]
                    }
                }
                await datasheet.records.aupdate([update_data])
                logger.info(f"成功将附件关联到记录 {record_id}")
                return (
                    f"✅ 文件上传并成功关联！\n\n"
                    f"📄 **文件名**: {attachment.name}\n"
                    f"📦 **大小**: {attachment.size / 1024:.2f} KB\n"
                    f"🔗 **已关联到**: 数据表 '{datasheet_name}' -> 记录 '{record_id}' -> 字段 '{field_name}'"
                )

            # 如果没有提供记录ID，则只返回附件信息
            result = (
                f"✅ 文件上传成功！\n\n"
                f"📄 **文件名**: {attachment.name}\n"
                f"📦 **大小**: {attachment.size / 1024:.2f} KB\n"
                f"🔗 **URL**: {attachment.url}\n"
                f"🔑 **附件Token**: `{attachment.token}`\n\n"
                f"💡 **提示**: 您可以在'添加记录'或'更新记录'时，在相应的附件字段中使用此附件Token。"
            )
            return result

        except Exception as e:
            error_msg = f"❌ 上传附件到 '{datasheet_name}' 失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    async def _send_status_feedback(self, message: str):
        """发送状态反馈给用户"""
        try:
            if self.context and hasattr(self.context, 'event') and self.context.event:
                await self.context.event.reply(f"【维格表】{message}")
            else:
                logger.debug(f"无法发送状态反馈（event不可用）: {message}")
        except Exception as e:
            logger.error(f"发送状态反馈失败: {e}")

    async def _delayed_auto_sync(self):
        """延迟执行自动同步，以确保event对象已准备好"""
        await asyncio.sleep(0.1)  # 短暂延迟
        try:
            await self._auto_sync_if_needed()
        except Exception as e:
            logger.error(f"延迟自动同步失败: {e}")
            await self._send_status_feedback(f"自动同步失败: {e}")

    async def initialize(self):
        """插件初始化"""
        logger.info("维格表MCP插件初始化完成")
        # 使用延迟任务来确保event对象可用
        asyncio.create_task(self._delayed_auto_sync())

    async def terminate(self):
        """插件销毁"""
        logger.info("维格表MCP插件已销毁")
