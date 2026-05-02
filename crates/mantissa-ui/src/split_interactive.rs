use anyhow::Result;
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
use std::collections::{HashMap, HashSet};
use std::io::{self, Stdout};
use std::time::Duration;
use uuid::Uuid;

/// Candidate node details consumed by the interactive split planner.
#[derive(Clone, Debug)]
pub struct SplitCandidate {
    pub node_id: Uuid,
    pub hostname: String,
    pub address: String,
    pub health: String,
    pub active_view: String,
    pub cpu_vendor: Option<String>,
    pub cpu_brand: Option<String>,
    pub cpu_logical: Option<u64>,
    pub cpu_cores: Option<u64>,
    pub memory_total_kb: Option<u64>,
    pub gpu_vendor: Option<String>,
    pub gpu_count: Option<u64>,
    pub gpu_models: Vec<String>,
    pub wireguard_enabled: bool,
    pub labels: Vec<String>,
}

/// Payload rendered by the split planner UI.
#[derive(Clone, Debug)]
pub struct SplitCandidateList {
    pub source_view: String,
    pub candidates: Vec<SplitCandidate>,
}

/// One named split target returned by the interactive planner.
#[derive(Clone, Debug)]
pub struct InteractiveSplitTarget {
    pub name: String,
    pub node_ids: Vec<Uuid>,
}

/// Final selection returned by the interactive planner.
#[derive(Clone, Debug)]
pub struct InteractiveSplitSelection {
    pub targets: Vec<InteractiveSplitTarget>,
    pub cancelled: bool,
}

/// Keyboard focus pane in the planner UI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FocusPane {
    Nodes,
    Groups,
}

/// Mutable UI state used by the split planner event loop.
struct SplitPlannerApp {
    source_view: String,
    candidates: Vec<SplitCandidate>,
    group_names: Vec<String>,
    active_group: usize,
    query: String,
    search_mode: bool,
    rename_mode: bool,
    rename_input: String,
    focus: FocusPane,
    status: String,
    filtered: Vec<usize>,
    selected: usize,
    scroll: usize,
    assignments: HashMap<Uuid, usize>,
    last_list_inner_area: Rect,
    last_group_inner_area: Rect,
    confirmed: bool,
    done: bool,
}

impl SplitPlannerApp {
    /// Creates planner state with deterministic default assignments and full candidate visibility.
    fn new(payload: SplitCandidateList, initial_group_names: &[String]) -> Self {
        let group_names = sanitize_initial_group_names(initial_group_names);
        let mut assignments = HashMap::with_capacity(payload.candidates.len());
        for (idx, candidate) in payload.candidates.iter().enumerate() {
            assignments.insert(candidate.node_id, idx % group_names.len());
        }

        let mut app = Self {
            source_view: payload.source_view,
            candidates: payload.candidates,
            group_names,
            active_group: 0,
            query: String::new(),
            search_mode: false,
            rename_mode: false,
            rename_input: String::new(),
            focus: FocusPane::Nodes,
            status: String::from(
                "Arrows move/select, Space assign active group, n new group, r rename, Enter confirm, q cancel",
            ),
            filtered: Vec::new(),
            selected: 0,
            scroll: 0,
            assignments,
            last_list_inner_area: Rect::default(),
            last_group_inner_area: Rect::default(),
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
                candidate.labels.join(","),
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

    /// Assigns the selected candidate to the currently active group.
    fn assign_selected_to_active_group(&mut self) {
        if let Some(candidate) = self.selected_candidate() {
            self.assignments
                .insert(candidate.node_id, self.active_group);
        }
    }

    /// Assigns the selected candidate to the provided group index when it exists.
    fn assign_selected_to_group(&mut self, group_index: usize) {
        if group_index >= self.group_names.len() {
            return;
        }
        if let Some(candidate) = self.selected_candidate() {
            self.assignments.insert(candidate.node_id, group_index);
        }
    }

    /// Moves the selected candidate to the next/previous group index.
    fn cycle_selected_group(&mut self, delta: i32) {
        if self.group_names.is_empty() {
            return;
        }
        if let Some(candidate) = self.selected_candidate() {
            let current = self.assignment_for(candidate.node_id);
            let next = if delta < 0 {
                current
                    .checked_sub(delta.unsigned_abs() as usize)
                    .unwrap_or(self.group_names.len() - 1)
            } else {
                (current + delta as usize) % self.group_names.len()
            };
            self.assignments.insert(candidate.node_id, next);
        }
    }

    /// Returns the assignment group index for one node id.
    fn assignment_for(&self, node_id: Uuid) -> usize {
        self.assignments.get(&node_id).copied().unwrap_or(0)
    }

    /// Returns assignment counts for each configured group.
    fn assignment_counts(&self) -> Vec<usize> {
        let mut counts = vec![0usize; self.group_names.len()];
        for candidate in &self.candidates {
            let group = self.assignment_for(candidate.node_id);
            if let Some(slot) = counts.get_mut(group) {
                *slot = slot.saturating_add(1);
            }
        }
        counts
    }

    /// Moves active-group focus forward/backward for assignment and group actions.
    fn move_active_group(&mut self, delta: i32) {
        if self.group_names.is_empty() {
            self.active_group = 0;
            return;
        }

        self.active_group = if delta < 0 {
            self.active_group
                .checked_sub(delta.unsigned_abs() as usize)
                .unwrap_or(self.group_names.len() - 1)
        } else {
            (self.active_group + delta as usize) % self.group_names.len()
        };
    }

    /// Creates a new group with a deterministic unique name and focuses it.
    fn create_group(&mut self) {
        let mut seen = self.group_names.iter().cloned().collect::<HashSet<_>>();
        let mut suffix = self.group_names.len() + 1;
        let name = loop {
            let candidate = format!("group-{suffix}");
            if seen.insert(candidate.clone()) {
                break candidate;
            }
            suffix = suffix.saturating_add(1);
        };

        self.group_names.push(name.clone());
        self.active_group = self.group_names.len() - 1;
        self.focus = FocusPane::Groups;
        self.status = format!("created group '{name}'");
    }

    /// Starts inline rename mode for the currently active group.
    fn begin_rename_active_group(&mut self) {
        if self.group_names.is_empty() {
            return;
        }

        self.rename_input = self.group_names[self.active_group].clone();
        self.rename_mode = true;
        self.status = String::from("rename mode: Enter apply, Esc cancel");
    }

    /// Applies the currently typed rename input to the active group with uniqueness checks.
    fn apply_group_rename(&mut self) {
        if self.group_names.is_empty() {
            self.rename_mode = false;
            self.rename_input.clear();
            return;
        }

        let trimmed = self.rename_input.trim().to_string();
        if trimmed.is_empty() {
            self.status = String::from("group name must not be empty");
            return;
        }
        let duplicate = self
            .group_names
            .iter()
            .enumerate()
            .any(|(idx, name)| idx != self.active_group && name == &trimmed);
        if duplicate {
            self.status = format!("group name '{trimmed}' already exists");
            return;
        }

        self.group_names[self.active_group] = trimmed.clone();
        self.rename_mode = false;
        self.rename_input.clear();
        self.status = format!("renamed group to '{trimmed}'");
    }

    /// Deletes the active group when safe and keeps assignment indices stable.
    fn delete_active_group(&mut self) {
        if self.group_names.len() <= 2 {
            self.status = String::from("at least two groups are required");
            return;
        }

        let counts = self.assignment_counts();
        if counts.get(self.active_group).copied().unwrap_or_default() > 0 {
            self.status = String::from("move nodes out of the group before deleting it");
            return;
        }

        let removed_index = self.active_group;
        let removed_name = self.group_names.remove(removed_index);
        for assignment in self.assignments.values_mut() {
            if *assignment > removed_index {
                *assignment -= 1;
            }
            if *assignment == removed_index {
                *assignment = 0;
            }
        }

        if self.active_group >= self.group_names.len() {
            self.active_group = self.group_names.len().saturating_sub(1);
        }
        self.status = format!("deleted group '{removed_name}'");
    }

    /// Finalizes split selection when all groups are non-empty and at least two groups exist.
    fn confirm(&mut self) {
        if self.group_names.len() < 2 {
            self.status = String::from("split requires at least two groups");
            return;
        }

        let counts = self.assignment_counts();
        if counts.contains(&0) {
            self.status =
                String::from("each group must contain at least one node before confirming");
            return;
        }

        self.confirmed = true;
        self.done = true;
    }

    /// Handles one key event in normal, search, or rename modes.
    fn on_key(&mut self, key: KeyEvent) {
        if self.rename_mode {
            match key.code {
                KeyCode::Esc => {
                    self.rename_mode = false;
                    self.rename_input.clear();
                    self.status = String::from("rename cancelled");
                }
                KeyCode::Enter => self.apply_group_rename(),
                KeyCode::Backspace => {
                    self.rename_input.pop();
                }
                KeyCode::Char(ch) => {
                    self.rename_input.push(ch);
                }
                _ => {}
            }
            return;
        }

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
            KeyCode::Tab => {
                self.focus = match self.focus {
                    FocusPane::Nodes => FocusPane::Groups,
                    FocusPane::Groups => FocusPane::Nodes,
                };
            }
            KeyCode::Up | KeyCode::Char('k') => match self.focus {
                FocusPane::Nodes => self.move_selection(-1),
                FocusPane::Groups => self.move_active_group(-1),
            },
            KeyCode::Down | KeyCode::Char('j') => match self.focus {
                FocusPane::Nodes => self.move_selection(1),
                FocusPane::Groups => self.move_active_group(1),
            },
            KeyCode::Left | KeyCode::Char('h') => self.cycle_selected_group(-1),
            KeyCode::Right | KeyCode::Char('l') => self.cycle_selected_group(1),
            KeyCode::Char(' ') | KeyCode::Char('a') => self.assign_selected_to_active_group(),
            KeyCode::Char('[') => self.move_active_group(-1),
            KeyCode::Char(']') => self.move_active_group(1),
            KeyCode::Char('n') => self.create_group(),
            KeyCode::Char('r') => self.begin_rename_active_group(),
            KeyCode::Char('x') => self.delete_active_group(),
            KeyCode::Char('/') => {
                self.search_mode = true;
                self.status = String::from("search mode: type to filter, Enter to apply");
            }
            KeyCode::Char('c') => {
                self.query.clear();
                self.refresh_filter();
                self.status = String::from("search filter cleared");
            }
            KeyCode::Char(ch) if ch.is_ascii_digit() => {
                let index = ch.to_digit(10).unwrap_or_default() as usize;
                if index == 0 {
                    return;
                }
                self.assign_selected_to_group(index - 1);
                self.active_group = self
                    .active_group
                    .min(self.group_names.len().saturating_sub(1));
            }
            KeyCode::Enter => self.confirm(),
            _ => {}
        }
    }

    /// Handles mouse hover/click updates over node and group panes.
    fn on_mouse(&mut self, mouse: MouseEvent) {
        let supports_select = matches!(
            mouse.kind,
            MouseEventKind::Moved | MouseEventKind::Down(MouseButton::Left)
        );
        if !supports_select {
            return;
        }

        if !self.filtered.is_empty() {
            let area = self.last_list_inner_area;
            if area.width > 0
                && area.height > 0
                && mouse.column >= area.x
                && mouse.column < area.right()
                && mouse.row >= area.y
                && mouse.row < area.bottom()
            {
                let row_offset = (mouse.row - area.y) as usize;
                let new_selected = self.scroll.saturating_add(row_offset);
                if new_selected < self.filtered.len() {
                    self.selected = new_selected;
                    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        self.assign_selected_to_active_group();
                    }
                }
            }
        }

        let group_area = self.last_group_inner_area;
        if group_area.width == 0 || group_area.height == 0 {
            return;
        }
        if mouse.column < group_area.x || mouse.column >= group_area.right() {
            return;
        }
        if mouse.row < group_area.y || mouse.row >= group_area.bottom() {
            return;
        }

        let group_row = (mouse.row - group_area.y) as usize;
        if group_row < self.group_names.len() {
            self.active_group = group_row;
            self.focus = FocusPane::Groups;
        }
    }

    /// Returns the final split selection after the loop exits.
    fn outcome(&self) -> InteractiveSplitSelection {
        if !self.confirmed {
            return InteractiveSplitSelection {
                targets: Vec::new(),
                cancelled: true,
            };
        }

        let mut targets = Vec::with_capacity(self.group_names.len());
        for (group_idx, group_name) in self.group_names.iter().enumerate() {
            let mut node_ids = self
                .candidates
                .iter()
                .filter_map(|candidate| {
                    if self.assignment_for(candidate.node_id) == group_idx {
                        Some(candidate.node_id)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            node_ids.sort();

            targets.push(InteractiveSplitTarget {
                name: group_name.clone(),
                node_ids,
            });
        }

        InteractiveSplitSelection {
            targets,
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
        .title(format!("Split Planner {}", app.source_view))
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
        let assign_index = app.assignment_for(candidate.node_id);
        let assign_tag = format!("{:>2}", assign_index + 1);
        let assign_name = app
            .group_names
            .get(assign_index)
            .map(|name| truncate(name, 10))
            .unwrap_or_else(|| String::from("-"));
        let cpu = candidate
            .cpu_vendor
            .clone()
            .unwrap_or_else(|| String::from("-"));
        let mem = format_kib(candidate.memory_total_kb);
        let gpu = format_gpu(candidate);
        items.push(ListItem::new(format!(
            "[{assign_tag}:{assign_name}] {:8} {:20} {:18} {:9} {:10} {} {}",
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

    let group_counts = app.assignment_counts();
    let group_lines = app
        .group_names
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let marker = if idx == app.active_group { "*" } else { " " };
            let count = group_counts.get(idx).copied().unwrap_or_default();
            format!("{marker} {}. {} ({count})", idx + 1, name)
        })
        .collect::<Vec<_>>()
        .join("\n");

    let mode = if app.rename_mode {
        "Rename"
    } else if app.search_mode {
        "Search"
    } else {
        "Normal"
    };
    let focus = match app.focus {
        FocusPane::Nodes => "Nodes",
        FocusPane::Groups => "Groups",
    };
    let query_display = if app.query.is_empty() {
        String::from("<none>")
    } else {
        app.query.clone()
    };

    let groups_block = Block::default().title("Groups").borders(Borders::ALL);
    let groups_inner = groups_block.inner(top_chunks[1]);
    app.last_group_inner_area = groups_inner;

    let summary = Paragraph::new(format!(
        "Mode: {mode}\nFocus: {focus}\nQuery: {query_display}\n\n{group_lines}\n\nRename input: {}\n\nKeys:\n  Tab switch focus\n  ↑/↓ move selection\n  ←/→ cycle node group\n  1..9 assign group\n  Space assign active group\n  [ ] change active group\n  n new group\n  r rename active group\n  x delete empty group\n  / search\n  Enter confirm\n  q cancel\n\nStatus:\n{}",
        if app.rename_mode {
            app.rename_input.clone()
        } else {
            String::from("<none>")
        },
        app.status
    ))
    .block(groups_block)
    .wrap(Wrap { trim: false });
    frame.render_widget(summary, top_chunks[1]);

    let details = if let Some(candidate) = app.selected_candidate() {
        format!(
            "Node: {}\nHostname: {}\nAddress: {}\nHealth: {}\nActive view: {}\nWireGuard: {}\nLabels: {}\nCPU vendor: {}\nCPU brand: {}\nCPU cores/logical: {}/{}\nMemory: {}\nGPU vendor: {}\nGPU count: {}\nGPU models: {}",
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
            format_candidate_labels(candidate),
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

/// Runs the interactive split planner and returns either named partition targets or a cancel result.
pub fn run_split_planner(
    payload: SplitCandidateList,
    initial_group_names: &[String],
) -> Result<InteractiveSplitSelection> {
    enable_raw_mode()?;
    let mut stdout: Stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = SplitPlannerApp::new(payload, initial_group_names);
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

/// Formats one split candidate label list for the node-details pane.
fn format_candidate_labels(candidate: &SplitCandidate) -> String {
    if candidate.labels.is_empty() {
        String::from("-")
    } else {
        candidate.labels.join(", ")
    }
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

/// Builds initial split group names from CLI defaults while enforcing non-empty unique names.
fn sanitize_initial_group_names(initial_group_names: &[String]) -> Vec<String> {
    let mut seen = HashSet::<String>::new();
    let mut groups = Vec::new();

    for name in initial_group_names {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            continue;
        }

        let unique = reserve_unique_group_name(&mut seen, trimmed);
        groups.push(unique);
    }

    while groups.len() < 2 {
        let fallback = format!("group-{}", groups.len() + 1);
        let unique = reserve_unique_group_name(&mut seen, &fallback);
        groups.push(unique);
    }

    groups
}

/// Reserves one unique group name, appending a numeric suffix when needed.
fn reserve_unique_group_name(seen: &mut HashSet<String>, preferred: &str) -> String {
    if seen.insert(preferred.to_string()) {
        return preferred.to_string();
    }

    let mut suffix = 2u32;
    loop {
        let candidate = format!("{preferred}-{suffix}");
        if seen.insert(candidate.clone()) {
            return candidate;
        }
        suffix = suffix.saturating_add(1);
    }
}
