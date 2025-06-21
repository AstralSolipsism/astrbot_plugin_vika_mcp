import asyncio
import json
import traceback
import os
import time
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta

from astrbot.api.event import filter, AstrMessageEvent
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
        
        # 智能数据表管理
        self.datasheet_mapping = {}  # 自动发现的数据表映射 {name: id}
        self.spaces_list = []  # 空间站列表
        self.default_space_id = None
        self.cache_file_path = os.path.join(os.getcwd(), "data", "vika_cache.json")
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
            self.plugin_config = self.config.get_plugin_config(plugin_name) or {}
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
            self.vika_client = Vika(api_token, host=vika_host)
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
                self.spaces_list = cache_data.get('spaces_list', [])
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
                'spaces_list': self.spaces_list,
                'timestamp': time.time()
            }
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            logger.info("缓存已保存")
        except Exception as e:
            logger.error(f"保存缓存失败: {e}")

    async def _run_sync_in_thread(self, func, *args, **kwargs):
        """在线程池中异步执行同步函数"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args, **kwargs)

    def _get_datasheet_id(self, datasheet_name: str) -> str:
        """智能获取数据表ID"""
        # 1. 首先检查自动发现的映射
        if datasheet_name in self.datasheet_mapping:
            return self.datasheet_mapping[datasheet_name]
            
        # 2. 检查自定义别名
        custom_aliases = self.plugin_config.get('custom_aliases', {})
        if datasheet_name in custom_aliases:
            real_name = custom_aliases[datasheet_name]
            if real_name in self.datasheet_mapping:
                return self.datasheet_mapping[real_name]
                
        # 3. 检查手动配置的映射（向后兼容）
        manual_mapping = self.plugin_config.get('datasheet_mapping', {})
        if datasheet_name in manual_mapping:
            return manual_mapping[datasheet_name]
            
        # 4. 如果是直接的datasheet ID格式，直接返回
        if datasheet_name.startswith('dst'):
            return datasheet_name
            
        # 5. 模糊匹配（部分匹配）
        for name, ds_id in self.datasheet_mapping.items():
            if datasheet_name.lower() in name.lower() or name.lower() in datasheet_name.lower():
                return ds_id
                
        # 6. 都找不到，返回原名称
        return datasheet_name

    def _get_datasheet(self, datasheet_name: str):
        """获取数据表对象"""
        if not self.vika_client:
            raise ValueError("维格表客户端未初始化，请检查API Token和服务器配置")
        
        datasheet_id = self._get_datasheet_id(datasheet_name)
        if not datasheet_id:
            raise ValueError(f"未找到数据表: {datasheet_name}")
        
        return self.vika_client.datasheet(datasheet_id)
        
    async def _traverse_node_recursive(self, node_id: str, datasheet_map: Dict[str, str], depth: int = 0):
        """递归遍历节点，发现所有数据表"""
        if depth > 10:  # 防止无限递归
            logger.warning(f"递归深度超过限制，停止遍历节点: {node_id}")
            return
            
        try:
            node_detail = await self._run_sync_in_thread(
                lambda: self.vika_client.nodes.get(node_id)
            )
            
            if hasattr(node_detail, 'type'):
                if node_detail.type == 'Datasheet':
                    datasheet_map[node_detail.name] = node_detail.id
                    logger.info(f"发现数据表: {node_detail.name} ({node_detail.id})")
                elif node_detail.type == 'Folder' and hasattr(node_detail, 'children'):
                    for child in node_detail.children:
                        await self._traverse_node_recursive(child.get('id'), datasheet_map, depth + 1)
        except Exception as e:
            logger.error(f"遍历节点 {node_id} 时出错: {e}")

    async def _auto_sync_if_needed(self):
        """如果需要且配置允许，自动同步数据表"""
        if (not self.is_synced and 
            self.plugin_config.get('auto_sync_on_startup', True) and 
            self.vika_client):
            try:
                await self._perform_sync()
            except Exception as e:
                logger.error(f"自动同步失败: {e}")

    async def _perform_sync(self):
        """执行数据表同步"""
        if not self.vika_client:
            raise ValueError("维格表客户端未初始化")
            
        # 获取空间站列表
        spaces = await self._run_sync_in_thread(
            lambda: list(self.vika_client.spaces.all())
        )
        
        if not spaces:
            raise ValueError("未找到任何空间站，请检查API Token权限")
            
        self.spaces_list = [{'id': space.id, 'name': space.name} for space in spaces]
        
        # 确定要同步的空间站
        target_space = None
        if self.default_space_id:
            target_space = next((s for s in spaces if s.id == self.default_space_id), None)
        
        if not target_space:
            target_space = spaces[0]  # 使用第一个空间站
            
        logger.info(f"同步空间站: {target_space.name} ({target_space.id})")
        
        # 获取根节点并递归遍历
        root_nodes = await self._run_sync_in_thread(
            lambda: list(self.vika_client.space(target_space.id).nodes.all())
        )
        
        new_datasheet_map = {}
        for node in root_nodes:
            await self._traverse_node_recursive(node.id, new_datasheet_map)
            
        self.datasheet_mapping.update(new_datasheet_map)
        self.is_synced = True
        self.cache_timestamp = time.time()
        
        # 保存到缓存
        self._save_cache()
        
        return len(new_datasheet_map)

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
                if 'fields' in record:
                    all_fields.update(record['fields'].keys())
            
            field_list = list(all_fields)
            
            # 表头
            result += "| " + " | ".join(field_list) + " |\\n"
            result += "|" + "---|" * len(field_list) + "\\n"
            
            # 数据行
            for record in display_records:
                fields = record.get('fields', {})
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

    def _parse_field_data(self, field_data_str: str) -> Dict[str, Any]:
        """解析字段数据字符串为字典"""
        try:
            # 尝试解析JSON格式
            return json.loads(field_data_str)
        except json.JSONDecodeError:
            # 如果不是JSON，尝试解析键值对格式
            fields = {}
            pairs = field_data_str.split(',')
            for pair in pairs:
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    key = key.strip().strip('"').strip("'")
                    value = value.strip().strip('"').strip("'")
                    
                    # 自动类型转换
                    if self.plugin_config.get('enable_auto_type_conversion', True):
                        if value.isdigit():
                            value = int(value)
                        elif value.replace('.', '').isdigit():
                            value = float(value)
                        elif value.lower() in ['true', 'false']:
                            value = value.lower() == 'true'
                    
                    fields[key] = value
            return fields

    @filter.on_llm_request()
    async def on_startup_sync(self, event):
        """插件启动时自动同步"""
        await self._auto_sync_if_needed()

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
        """列出您有权访问的所有维格表空间站。
        """
        if not self.vika_client:
            return "❌ 错误：维格表客户端未初始化，请检查API Token配置"
            
        try:
            spaces = await self._run_sync_in_thread(
                lambda: list(self.vika_client.spaces.all())
            )
            
            if not spaces:
                return "📭 未找到任何空间站，请检查您的API Token权限"
                
            result = f"🏢 **您的维格表空间站** (共 {len(spaces)} 个)：\n\n"
            
            for space in spaces:
                is_default = " 🔸 *默认*" if space.id == self.default_space_id else ""
                result += f"• **{space.name}**{is_default}\n"
                result += f"  ID: `{space.id}`\n\n"
                
            if not self.default_space_id and len(spaces) > 1:
                result += "💡 **提示**: 如果您有多个空间站，建议在配置中设置 `default_space_id` 以指定默认操作的空间站。"
                
            return result
            
        except Exception as e:
            error_msg = f"❌ 获取空间站列表失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="list_vika_datasheets")
    async def list_vika_datasheets(self, event: AstrMessageEvent, filter_keyword: str = None) -> str:
        """列出所有已发现的维格表数据表，支持关键词过滤。

        Args:
            filter_keyword (str): 可选，过滤关键词，只显示包含该关键词的数据表
        """
        if not self.is_synced:
            return "⚠️ 数据表列表尚未同步，请先运行数据表同步功能，或者稍等片刻让系统自动同步。"
            
        if not self.datasheet_mapping:
            return "📭 未发现任何数据表，请检查空间站中是否有数据表，或重新同步。"
            
        # 应用过滤
        filtered_tables = self.datasheet_mapping
        if filter_keyword:
            filtered_tables = {
                name: ds_id for name, ds_id in self.datasheet_mapping.items()
                if filter_keyword.lower() in name.lower()
            }
            
        if not filtered_tables:
            return f"🔍 未找到包含关键词 '{filter_keyword}' 的数据表。"
            
        result = f"📊 **数据表列表**"
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
                if real_name in self.datasheet_mapping:
                    result += f"• `{alias}` → {real_name}\n"
            result += "\n"
            
        result += "💡 **提示**: 您可以直接使用数据表名称进行操作，系统会自动识别。"
        
        return result

    @filter.llm_tool(name="create_vika_datasheet")
    async def create_vika_datasheet(
        self, 
        event: AstrMessageEvent, 
        name: str, 
        description: str = "",
        fields_config: str = ""
    ) -> str:
        """在维格表空间站中创建一个新的数据表。

        Args:
            name (str): 数据表名称（必需）
            description (str): 数据表描述（可选）
            fields_config (str): 字段配置，JSON格式，例如：'[{"name":"姓名","type":"SingleLineText"},{"name":"年龄","type":"Number"}]'
        """
        if not self.vika_client:
            return "❌ 错误：维格表客户端未初始化，请检查API Token配置"
            
        try:
            # 确定目标空间站
            spaces = await self._run_sync_in_thread(
                lambda: list(self.vika_client.spaces.all())
            )
            
            if not spaces:
                return "❌ 未找到任何空间站，请检查API Token权限"
                
            target_space = None
            if self.default_space_id:
                target_space = next((s for s in spaces if s.id == self.default_space_id), None)
            
            if not target_space:
                target_space = spaces[0]
                
            # 解析字段配置
            fields = []
            if fields_config:
                try:
                    fields = json.loads(fields_config)
                except json.JSONDecodeError:
                    return "❌ 字段配置格式错误，请使用正确的JSON格式"
                    
            # 创建数据表
            create_params = {
                'name': name,
                'description': description,
                'folderId': target_space.id
            }
            
            if fields:
                create_params['fields'] = fields
                
            new_datasheet = await self._run_sync_in_thread(
                lambda: self.vika_client.space(target_space.id).datasheets.create(**create_params)
            )
            
            # 更新本地缓存
            self.datasheet_mapping[name] = new_datasheet.id
            self._save_cache()
            
            result = f"✅ 数据表创建成功！\n\n"
            result += f"📊 **数据表名称**: {name}\n"
            result += f"🆔 **数据表ID**: `{new_datasheet.id}`\n"
            result += f"🏢 **所在空间站**: {target_space.name}\n"
            
            if description:
                result += f"📝 **描述**: {description}\n"
                
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
            
        # 尝试智能获取数据表ID
        datasheet_id = self._get_datasheet_id(datasheet_name)
        
        # 如果没有找到匹配的数据表，提供建议
        if datasheet_id == datasheet_name and not datasheet_name.startswith('dst'):
            if not self.is_synced:
                return f"⚠️ 未找到数据表 '{datasheet_name}'，且数据表列表尚未同步。请先运行数据表同步功能。"
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
                    return (f"❌ 未找到数据表 '{datasheet_name}'。\n"
                           f"可用的数据表：{', '.join(available_tables)}")
        
        return None  # 找到了数据表，无需提示

    @filter.llm_tool(name="get_vika_records")
    async def get_vika_records(
        self, 
        event: AstrMessageEvent, 
        datasheet_name: str, 
        view_name: str = None,
        filter_formula: str = None,
        max_records: int = None
    ) -> str:
        """从指定的维格表中获取记录数据。

        Args:
            datasheet_name (str): 要查询的数据表名称或别名（必需）
            view_name (str): 可选，指定视图名称
            filter_formula (str): 可选，过滤公式，用于筛选记录
            max_records (int): 可选，最大返回记录数，默认为配置中的值
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            # 准备查询参数
            query_params = {}
            if view_name:
                query_params['view'] = view_name
            if filter_formula:
                query_params['filterByFormula'] = filter_formula
            if max_records:
                query_params['maxRecords'] = min(max_records, 100)  # API限制
            
            # 在线程池中执行同步操作
            if query_params:
                records = await self._run_sync_in_thread(
                    lambda: list(datasheet.records.filter(**query_params))
                )
            else:
                records = await self._run_sync_in_thread(
                    lambda: list(datasheet.records.all())
                )
            
            return self._format_records_for_display(records, max_records)
            
        except Exception as e:
            error_msg = f"❌ 获取维格表记录失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="add_vika_record")
    async def add_vika_record(
        self, 
        event: AstrMessageEvent, 
        datasheet_name: str, 
        record_data: str
    ) -> str:
        """向指定的维格表中添加新记录。

        Args:
            datasheet_name (str): 要添加记录的数据表名称或别名（必需）
            record_data (str): 记录数据，可以是JSON格式或键值对格式（如："姓名=张三,年龄=25"或'{"姓名":"张三","年龄":25}'）
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            # 解析记录数据
            fields_data = self._parse_field_data(record_data)
            
            if not fields_data:
                return "❌ 错误：记录数据为空或格式不正确。请使用JSON格式或键值对格式（如：'姓名=张三,年龄=25'）"
            
            # 在线程池中执行同步操作
            result = await self._run_sync_in_thread(
                lambda: datasheet.records.create(fields_data)
            )
            
            return f"✅ 成功添加记录到数据表 '{datasheet_name}'，记录ID: {result.record_id}"
            
        except Exception as e:
            error_msg = f"❌ 添加维格表记录失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="search_vika_records")
    async def search_vika_records(
        self, 
        event: AstrMessageEvent, 
        datasheet_name: str, 
        search_query: str,
        search_fields: str = None
    ) -> str:
        """在指定的维格表中搜索包含特定内容的记录。

        Args:
            datasheet_name (str): 要搜索的数据表名称或别名（必需）
            search_query (str): 搜索关键词（必需）
            search_fields (str): 可选，指定要搜索的字段名，多个字段用逗号分隔
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            # 获取所有记录
            all_records = await self._run_sync_in_thread(
                lambda: list(datasheet.records.all())
            )
            
            # 执行搜索
            matching_records = []
            query_lower = search_query.lower()
            search_field_list = None
            
            if search_fields:
                search_field_list = [f.strip() for f in search_fields.split(',')]
            
            for record in all_records:
                fields = record.get('fields', {})
                
                # 确定要搜索的字段
                fields_to_search = search_field_list if search_field_list else fields.keys()
                
                # 在指定字段中搜索
                for field_name in fields_to_search:
                    if field_name in fields:
                        field_value = fields[field_name]
                        if isinstance(field_value, str) and query_lower in field_value.lower():
                            matching_records.append(record)
                            break
                        elif str(field_value).lower() == query_lower:
                            matching_records.append(record)
                            break
            
            if not matching_records:
                search_scope = f"在字段 [{search_fields}] 中" if search_fields else "在所有字段中"
                return f"🔍 {search_scope}没有找到包含 '{search_query}' 的记录"
            
            result = f"🔍 **搜索结果** (关键词: '{search_query}'):\n\n"
            result += self._format_records_for_display(matching_records)
            return result
            
        except Exception as e:
            error_msg = f"❌ 搜索维格表记录失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="update_vika_record")
    async def update_vika_record(
        self, 
        event: AstrMessageEvent, 
        datasheet_name: str, 
        record_id: str,
        update_data: str
    ) -> str:
        """更新指定维格表中的记录。

        Args:
            datasheet_name (str): 数据表名称或别名（必需）
            record_id (str): 要更新的记录ID（必需）
            update_data (str): 更新数据，格式同添加记录
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            # 解析更新数据
            fields_data = self._parse_field_data(update_data)
            
            if not fields_data:
                return "❌ 错误：更新数据为空或格式不正确"
            
            # 在线程池中执行同步操作
            await self._run_sync_in_thread(
                lambda: datasheet.records.update(record_id, fields_data)
            )
            
            return f"✅ 成功更新数据表 '{datasheet_name}' 中的记录 {record_id}"
            
        except Exception as e:
            error_msg = f"❌ 更新维格表记录失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="delete_vika_record")
    async def delete_vika_record(
        self, 
        event: AstrMessageEvent, 
        datasheet_name: str, 
        record_id: str
    ) -> str:
        """删除指定维格表中的记录。

        Args:
            datasheet_name (str): 数据表名称或别名（必需）
            record_id (str): 要删除的记录ID（必需）
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            # 在线程池中执行同步操作
            await self._run_sync_in_thread(
                lambda: datasheet.records.delete(record_id)
            )
            
            return f"✅ 成功删除数据表 '{datasheet_name}' 中的记录 {record_id}"
            
        except Exception as e:
            error_msg = f"❌ 删除维格表记录失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="get_vika_fields")
    async def get_vika_fields(
        self, 
        event: AstrMessageEvent, 
        datasheet_name: str
    ) -> str:
        """获取指定维格表的字段信息。

        Args:
            datasheet_name (str): 数据表名称或别名（必需）
        """
        try:
            # 检查同步状态并提供建议
            sync_check = await self._check_sync_and_suggest(datasheet_name)
            if sync_check:
                return sync_check
                
            datasheet = self._get_datasheet(datasheet_name)
            
            # 获取字段信息
            fields = await self._run_sync_in_thread(
                lambda: list(datasheet.fields.all())
            )
            
            if not fields:
                return f"📋 数据表 '{datasheet_name}' 没有字段信息"
            
            result = f"📋 **数据表 '{datasheet_name}' 的字段信息** (共 {len(fields)} 个字段):\n\n"
            for field in fields:
                field_name = field.get('name', '未知')
                field_type = field.get('type', '未知')
                field_desc = field.get('description', '')
                
                result += f"• **{field_name}** (类型: {field_type})"
                if field_desc:
                    result += f" - {field_desc}"
                result += "\n"
            
            result += "\n💡 您可以使用这些字段名来添加或更新记录。"
            return result
            
        except Exception as e:
            error_msg = f"❌ 获取维格表字段信息失败: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return error_msg

    @filter.llm_tool(name="get_vika_status")
    async def get_vika_status(self, event: AstrMessageEvent) -> str:
        """检查维格表插件的连接状态、配置信息和数据表同步状态。
        """
        try:
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

    async def initialize(self):
        """插件初始化"""
        logger.info("维格表MCP插件初始化完成")
        # 启动时自动同步
        await self._auto_sync_if_needed()

    async def terminate(self):
        """插件销毁"""
        logger.info("维格表MCP插件已销毁")
