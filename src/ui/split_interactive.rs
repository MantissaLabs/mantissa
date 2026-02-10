use anyhow::Result;
use client::cluster::{ClusterViewSpec, SplitCandidate, SplitCandidateList};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, MouseButton,
        MouseEvent, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};
use std::collections::HashMap;
use std::io::{self, Stdout};
use std::time::Duration;
use uuid::Uuid;

/// One side of the interactive split planner assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SplitSide {
    Left,
    Right,
}

impl SplitSide {
    /// Returns a short tag rendered in the assignment table.
    fn tag(self) -> &'static str {
        match self {
            Self::Left => "L",
            Self::Right => "R",
        }
    }
}

/// Final selection returned by the interactive planner.
#[derive(Clone, Debug)]
pub struct InteractiveSplitSelection {
    pub left_name: String,
    pub right_name: String,
    pub left_nodes: Vec<Uuid>,
    pub right_nodes: Vec<Uuid>,
    pub cancelled: bool,
}

/// Mutable UI state used by the split planner event loop.
struct SplitPlannerApp {
    source_view: ClusterViewSpec,
    candidates: Vec<SplitCandidate>,
    left_name: String,
    right_name: String,
    query: String,
    search_mode: bool,
    status: String,
    filtered: Vec<usize>,
    selected: usize,
    scroll: usize,
    assignments: HashMap<Uuid, SplitSide>,
    last_list_inner_area: Rect,
    confirmed: bool,
    done: bool,
}

impl SplitPlannerApp {
    /// Creates planner state with deterministic default assignments and full candidate visibility.
    fn new(payload: SplitCandidateList, left_name: &str, right_name: &str) -> Self {
        let mut assignments = HashMap::with_capacity(payload.candidates.len());
        for (idx, candidate) in payload.candidates.iter().enumerate() {
            let side = if idx % 2 == 0 {
                SplitSide::Left
            } else {
                SplitSide::Right
            };
            assignments.insert(candidate.node_id, side);
        }

        let mut app = Self {
            source_view: payload.source_view,
            candidates: payload.candidates,
            left_name: left_name.to_string(),
            right_name: right_name.to_string(),
            query: String::new(),
            search_mode: false,
            status: String::from(
                "Arrows move/select, ←/→ assign, Space toggles, / search, Enter confirm, q cancel",
            ),
            filtered: Vec::new(),
            selected: 0,
            scroll: 0,
            assignments,
            last_list_inner_area: Rect::default(),
            confirmed: false,
            done: false,
        };
        app.refresh_filter();
        app
    }

    /// Recomputes candidate filtering from the current search query while preserving selection.
    fn refresh_filter(&mut self) {
        let current_id = self
            .selected_candidate()
            .map(|candidate| candidate.node_id)
            .or_else(|| self.candidates.first().map(|candidate| candidate.node_id));
        self.filtered.clear();

        let query = self.query.to_ascii_lowercase();
        for (idx, candidate) in self.candidates.iter().enumerate() {
            if query.is_empty() {
                self.filtered.push(idx);
                continue;
            }

            let fields = [
                candidate.node_id.to_string(),
                candidate.hostname.clone(),
                candidate.address.clone(),
                candidate
                    .gpu_vendor
                    .clone()
                    .unwrap_or_else(|| String::from("-")),
                candidate
                    .cpu_vendor
                    .clone()
                    .unwrap_or_else(|| String::from("-")),
            ];
            if fields
                .iter()
                .any(|field| field.to_ascii_lowercase().contains(&query))
            {
                self.filtered.push(idx);
            }
        }

        if self.filtered.is_empty() {
            self.selected = 0;
            self.scroll = 0;
            return;
        }

        self.selected = current_id
            .and_then(|id| {
                self.filtered
                    .iter()
                    .position(|idx| self.candidates[*idx].node_id == id)
            })
            .unwrap_or(0);
        self.scroll = self.scroll.min(self.selected);
    }

    /// Returns the currently selected candidate, if any candidate is visible.
    fn selected_candidate(&self) -> Option<&SplitCandidate> {
        self.filtered
            .get(self.selected)
            .and_then(|idx| self.candidates.get(*idx))
    }

    /// Advances the current selection by one row up/down in the filtered candidate list.
    fn move_selection(&mut self, delta: i32) {
        if self.filtered.is_empty() {
            return;
        }

        if delta < 0 {
            self.selected = self.selected.saturating_sub(delta.unsigned_abs() as usize);
        } else {
            self.selected = (self.selected + delta as usize).min(self.filtered.len() - 1);
        }
    }

    /// Assigns the selected candidate to one side.
    fn assign_selected(&mut self, side: SplitSide) {
        if let Some(candidate) = self.selected_candidate() {
            self.assignments.insert(candidate.node_id, side);
        }
    }

    /// Toggles selected candidate assignment between left and right sides.
    fn toggle_selected(&mut self) {
        if let Some(candidate) = self.selected_candidate() {
            let next = match self.assignment_for(candidate.node_id) {
                SplitSide::Left => SplitSide::Right,
                SplitSide::Right => SplitSide::Left,
            };
            self.assignments.insert(candidate.node_id, next);
        }
    }

    /// Returns the assignment side for one node id.
    fn assignment_for(&self, node_id: Uuid) -> SplitSide {
        self.assignments
            .get(&node_id)
            .copied()
            .unwrap_or(SplitSide::Left)
    }

    /// Returns assignment counts for left/right split sides.
    fn assignment_counts(&self) -> (usize, usize) {
        let mut left = 0usize;
        let mut right = 0usize;
        for candidate in &self.candidates {
            match self.assignment_for(candidate.node_id) {
                SplitSide::Left => left = left.saturating_add(1),
                SplitSide::Right => right = right.saturating_add(1),
            }
        }
        (left, right)
    }

    /// Finalizes split selection if both sides are non-empty, otherwise keeps the UI running.
    fn confirm(&mut self) {
        let (left, right) = self.assignment_counts();
        if left == 0 || right == 0 {
            self.status = String::from("both sides need at least one node before confirming");
            return;
        }

        self.confirmed = true;
        self.done = true;
    }

    /// Handles one key event in either normal-navigation mode or search-input mode.
    fn on_key(&mut self, key: KeyEvent) {
        if self.search_mode {
            match key.code {
                KeyCode::Esc => {
                    self.search_mode = false;
                    self.status = String::from("search cancelled");
                }
                KeyCode::Enter => {
                    self.search_mode = false;
                    self.status = format!("filter applied ({} nodes)", self.filtered.len());
                }
                KeyCode::Backspace => {
                    self.query.pop();
                    self.refresh_filter();
                }
                KeyCode::Char(ch) => {
                    self.query.push(ch);
                    self.refresh_filter();
                }
                _ => {}
            }
            return;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.done = true;
                self.status = String::from("split cancelled");
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Left | KeyCode::Char('h') => self.assign_selected(SplitSide::Left),
            KeyCode::Right | KeyCode::Char('l') => self.assign_selected(SplitSide::Right),
            KeyCode::Char(' ') => self.toggle_selected(),
            KeyCode::Char('/') => {
                self.search_mode = true;
                self.status = String::from("search mode: type to filter, Enter to apply");
            }
            KeyCode::Char('c') => {
                self.query.clear();
                self.refresh_filter();
                self.status = String::from("search filter cleared");
            }
            KeyCode::Enter => self.confirm(),
            _ => {}
        }
    }

    /// Handles mouse hover/click updates so details follow pointer movement over the node list.
    fn on_mouse(&mut self, mouse: MouseEvent) {
        let supports_select = matches!(
            mouse.kind,
            MouseEventKind::Moved | MouseEventKind::Down(MouseButton::Left)
        );
        if !supports_select {
            return;
        }
        if self.filtered.is_empty() {
            return;
        }

        let area = self.last_list_inner_area;
        if area.width == 0 || area.height == 0 {
            return;
        }
        if mouse.column < area.x || mouse.column >= area.right() {
            return;
        }
        if mouse.row < area.y || mouse.row >= area.bottom() {
            return;
        }

        let row_offset = (mouse.row - area.y) as usize;
        let new_selected = self.scroll.saturating_add(row_offset);
        if new_selected < self.filtered.len() {
            self.selected = new_selected;
            if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                self.toggle_selected();
            }
        }
    }

    /// Returns the final split selection after the loop exits.
    fn outcome(&self) -> InteractiveSplitSelection {
        if !self.confirmed {
            return InteractiveSplitSelection {
                left_name: self.left_name.clone(),
                right_name: self.right_name.clone(),
                left_nodes: Vec::new(),
                right_nodes: Vec::new(),
                cancelled: true,
            };
        }

        let mut left_nodes = Vec::new();
        let mut right_nodes = Vec::new();
        for candidate in &self.candidates {
            match self.assignment_for(candidate.node_id) {
                SplitSide::Left => left_nodes.push(candidate.node_id),
                SplitSide::Right => right_nodes.push(candidate.node_id),
            }
        }
        left_nodes.sort();
        right_nodes.sort();

        InteractiveSplitSelection {
            left_name: self.left_name.clone(),
            right_name: self.right_name.clone(),
            left_nodes,
            right_nodes,
            cancelled: false,
        }
    }
}

/// Renders the split planner UI with node table, assignment summary and focused-node details.
fn draw(frame: &mut Frame<'_>, app: &mut SplitPlannerApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(frame.area());

    let top_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(72), Constraint::Percentage(28)])
        .split(chunks[0]);

    let list_block = Block::default()
        .title(format!(
            "Split Planner {} ({})",
            app.source_view.cluster_id, app.source_view.epoch
        ))
        .borders(Borders::ALL);
    let list_inner = list_block.inner(top_chunks[0]);

    let visible_rows = list_inner.height as usize;
    if app.selected < app.scroll {
        app.scroll = app.selected;
    }
    if visible_rows > 0 && app.selected >= app.scroll.saturating_add(visible_rows) {
        app.scroll = app.selected.saturating_sub(visible_rows - 1);
    }
    if app.filtered.len() < app.scroll {
        app.scroll = 0;
    }
    app.last_list_inner_area = list_inner;

    let end = app
        .scroll
        .saturating_add(visible_rows)
        .min(app.filtered.len());
    let mut items = Vec::with_capacity(end.saturating_sub(app.scroll));
    for filtered_idx in app.scroll..end {
        let candidate = &app.candidates[app.filtered[filtered_idx]];
        let assign = app.assignment_for(candidate.node_id).tag();
        let cpu = candidate
            .cpu_vendor
            .clone()
            .unwrap_or_else(|| String::from("-"));
        let mem = format_kib(candidate.memory_total_kb);
        let gpu = format_gpu(candidate);
        items.push(ListItem::new(format!(
            "[{assign}] {:8} {:20} {:18} {:9} {:10} {} {}",
            short_uuid(candidate.node_id),
            truncate(&candidate.hostname, 20),
            truncate(&candidate.address, 18),
            candidate.health,
            truncate(&cpu, 10),
            mem,
            gpu
        )));
    }

    let mut list_state = ListState::default();
    if app.selected >= app.scroll && app.selected < end {
        list_state.select(Some(app.selected - app.scroll));
    }
    let list = List::new(items)
        .block(list_block)
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");
    frame.render_stateful_widget(list, top_chunks[0], &mut list_state);

    let (left_count, right_count) = app.assignment_counts();
    let summary = Paragraph::new(format!(
        "Query: {}\nMode: {}\n\n{}: {}\n{}: {}\nTotal: {}\n\nKeys:\n  ↑/↓ select\n  ←/→ assign\n  Space toggle\n  / search\n  Enter confirm\n  q cancel\n\nStatus:\n{}",
        if app.query.is_empty() { String::from("<none>") } else { app.query.clone() },
        if app.search_mode { "Search" } else { "Normal" },
        app.left_name,
        left_count,
        app.right_name,
        right_count,
        app.candidates.len(),
        app.status
    ))
    .block(Block::default().title("Summary").borders(Borders::ALL))
    .wrap(Wrap { trim: false });
    frame.render_widget(summary, top_chunks[1]);

    let details = if let Some(candidate) = app.selected_candidate() {
        format!(
            "Node: {}\nHostname: {}\nAddress: {}\nHealth: {}\nActive view: {}\nWireGuard: {}\nCPU vendor: {}\nCPU brand: {}\nCPU cores/logical: {}/{}\nMemory: {}\nGPU vendor: {}\nGPU count: {}\nGPU models: {}",
            candidate.node_id,
            candidate.hostname,
            candidate.address,
            candidate.health,
            candidate.active_view,
            if candidate.wireguard_enabled {
                "enabled"
            } else {
                "disabled"
            },
            candidate.cpu_vendor.as_deref().unwrap_or("-"),
            candidate.cpu_brand.as_deref().unwrap_or("-"),
            candidate
                .cpu_cores
                .map(|value| value.to_string())
                .unwrap_or_else(|| String::from("-")),
            candidate
                .cpu_logical
                .map(|value| value.to_string())
                .unwrap_or_else(|| String::from("-")),
            format_kib(candidate.memory_total_kb),
            candidate.gpu_vendor.as_deref().unwrap_or("-"),
            candidate
                .gpu_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| String::from("0")),
            if candidate.gpu_models.is_empty() {
                String::from("-")
            } else {
                candidate.gpu_models.join(", ")
            }
        )
    } else {
        String::from("No nodes match the current filter.")
    };
    let details_widget = Paragraph::new(details)
        .block(Block::default().title("Node Details").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(details_widget, chunks[1]);
}

/// Runs the interactive split planner and returns either a node partition or a cancel result.
pub fn run_split_planner(
    payload: SplitCandidateList,
    left_name: &str,
    right_name: &str,
) -> Result<InteractiveSplitSelection> {
    enable_raw_mode()?;
    let mut stdout: Stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = SplitPlannerApp::new(payload, left_name, right_name);
    let outcome = (|| -> Result<InteractiveSplitSelection> {
        while !app.done {
            terminal.draw(|frame| draw(frame, &mut app))?;

            if !event::poll(Duration::from_millis(200))? {
                continue;
            }

            match event::read()? {
                Event::Key(key) => app.on_key(key),
                Event::Mouse(mouse) => app.on_mouse(mouse),
                Event::Resize(_, _) => {}
                Event::FocusGained | Event::FocusLost | Event::Paste(_) => {}
            }
        }
        Ok(app.outcome())
    })();

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    outcome
}

/// Converts KiB values into compact human-readable text for split summary rendering.
fn format_kib(value: Option<u64>) -> String {
    let Some(kib) = value else {
        return String::from("-");
    };

    let mib = kib as f64 / 1024.0;
    if mib < 1024.0 {
        return format!("{mib:.0}MiB");
    }

    let gib = mib / 1024.0;
    format!("{gib:.1}GiB")
}

/// Builds one compact GPU descriptor string for the split candidate list table.
fn format_gpu(candidate: &SplitCandidate) -> String {
    let count = candidate.gpu_count.unwrap_or(0);
    if count == 0 {
        return String::from("gpu=0");
    }

    let vendor = candidate.gpu_vendor.as_deref().unwrap_or("gpu");
    format!("{}:{count}", truncate(vendor, 10))
}

/// Returns a stable short ID for compact list rows.
fn short_uuid(value: Uuid) -> String {
    let text = value.to_string();
    text.chars().take(8).collect()
}

/// Truncates long table fields while keeping deterministic text width.
fn truncate(value: &str, max_len: usize) -> String {
    if value.chars().count() <= max_len {
        return value.to_string();
    }

    let mut out = String::with_capacity(max_len);
    for ch in value.chars().take(max_len.saturating_sub(1)) {
        out.push(ch);
    }
    out.push('~');
    out
}
