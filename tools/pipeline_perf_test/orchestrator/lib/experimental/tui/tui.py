from textual.app import App, ComposeResult
from textual.widgets import Tree
from textual.containers import Container
from textual.timer import Timer
import asyncio

from rich.text import Text
from opentelemetry.trace import StatusCode


class TestSpanTUI(App):
    CSS_PATH = None

    def __init__(self, span_processor):
        super().__init__()
        self.span_processor = span_processor
        self.update_timer: Timer | None = None
        self.user_expanded_nodes = set()  # span_ids user manually expanded
        self.user_collapsed_nodes = set()  # span_ids user manually collapsed
        self.suppress_tree_events = False

    async def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        if self.suppress_tree_events:
            return
        node = event.node
        if node.data:
            self.user_expanded_nodes.add(node.data)
            # Remove from collapsed if present
            self.user_collapsed_nodes.discard(node.data)

    async def on_tree_node_collapsed(self, event: Tree.NodeCollapsed) -> None:
        if self.suppress_tree_events:
            return
        node = event.node
        if node.data:
            self.user_collapsed_nodes.add(node.data)
            # Remove from expanded if present
            self.user_expanded_nodes.discard(node.data)

    def on_key(self, event):
        if event.key == "ctrl+c" or event.key == "q":
            self.exit()

    def compose(self) -> ComposeResult:
        yield Container(Tree("🔍 Spans", id="span_tree"))

    def on_mount(self) -> None:
        self.update_timer = self.set_interval(1, self.refresh_tree)

    def format_span(self, info: dict) -> Text:
        span = info["span"]
        name = span.name
        status_code = span.status.status_code
        is_active = info["status"] == "in_progress"

        if status_code == StatusCode.OK:
            icon = "✅"
            color = "green"
        elif status_code == StatusCode.ERROR:
            icon = "❌"
            color = "red"
        elif is_active:
            icon = "🔄"
            color = "cyan"
        else:
            icon = "⚪"
            color = "white"

        return Text(f"{icon} {name} {status_code}", style=color)

    async def refresh_tree(self):
        self.suppress_tree_events = True
        span_data = self.span_processor.get_snapshot()
        ## Example span_data construction
        # self.spans[span.context.span_id] = {
        #     "span": span,
        #     "start_time": span.start_time,
        #     "status": "in_progress",
        #     "parent_span_id": getattr(span.parent, "span_id", None),
        #     "events": [],
        # }
        tree_widget = self.query_one("#span_tree", Tree)

        # 🔁 Recursively collect expanded node span_ids
        def collect_expanded_ids(node):
            expanded = set()
            if node.is_expanded and node.data:
                expanded.add(node.data)
            for child in node.children:
                expanded |= collect_expanded_ids(child)
            return expanded

        # Clear tree
        tree_widget.root.label = "🧪 Test Spans"
        tree_widget.root.remove_children()

        # Rebuild tree structure
        parent_map = {}
        for span_id, info in span_data.items():
            parent_id = info.get("parent_span_id")
            parent_map.setdefault(parent_id, []).append(span_id)

        def build_subtree(parent_id, parent_node, level=0):
            has_error_in_subtree = False

            for child_id in parent_map.get(parent_id, []):
                info = span_data[child_id]
                label = self.format_span(info)
                node = parent_node.add(label, data=child_id)

                span = info["span"]
                is_active = info["status"] == "in_progress"
                has_error = span.status.status_code == StatusCode.ERROR

                # Recursively build children and check if they have error
                child_has_error = build_subtree(child_id, node, level + 1)

                # This node has error if it or any of its children have error
                node_has_error = has_error or child_has_error

                # Decide whether to expand or collapse this node
                if child_id in self.user_expanded_nodes:
                    node.expand()
                elif child_id in self.user_collapsed_nodes:
                    node.collapse()
                elif level < 2 or is_active or node_has_error:
                    node.expand()
                else:
                    node.collapse()

                # Propagate error presence upward
                if node_has_error:
                    has_error_in_subtree = True

            return has_error_in_subtree

        build_subtree(None, tree_widget.root, 0)
        await asyncio.sleep(0)
        self.suppress_tree_events = False

        tree_widget.root.expand()
