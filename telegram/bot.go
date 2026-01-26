package telegram

import (
	"encoding/json"
	"fmt"
	"nofx/logger"
	"nofx/manager"
	"nofx/store"
	"nofx/trader"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const authorizedChatsKey = "telegram_authorized_chats"

type Bot struct {
	manager       *manager.TraderManager
	store         *store.Store
	bot           *tgbotapi.BotAPI
	authPassword  string
	authChats     map[int64]struct{}
	authMu        sync.RWMutex
	chatTrader    map[int64]string
	chatTraderMu  sync.RWMutex
	decisionMu    sync.Mutex
	decisionAfter map[string]int64
}

// NewBot creates a Telegram bot instance if TELEGRAM_BOT_TOKEN is configured.
func NewBot(manager *manager.TraderManager, st *store.Store) (*Bot, error) {
	token := strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN"))
	if token == "" {
		logger.Info("📡 Telegram bot disabled (TELEGRAM_BOT_TOKEN not set)")
		return nil, nil
	}

	password := strings.TrimSpace(os.Getenv("TELEGRAM_AUTH_PASSWORD"))
	if password == "" {
		return nil, fmt.Errorf("TELEGRAM_AUTH_PASSWORD not set")
	}

	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("failed to create telegram bot: %w", err)
	}
	if _, err := api.Request(tgbotapi.DeleteWebhookConfig{DropPendingUpdates: true}); err != nil {
		logger.Warnf("⚠️ Telegram deleteWebhook failed: %v", err)
	}

	b := &Bot{
		manager:       manager,
		store:         st,
		bot:           api,
		authPassword:  password,
		authChats:     make(map[int64]struct{}),
		chatTrader:    make(map[int64]string),
		decisionAfter: make(map[string]int64),
	}
	b.loadAuthorizedChats()
	b.initDecisionCursors()
	logger.Infof("📡 Telegram bot enabled: @%s", api.Self.UserName)
	return b, nil
}

// Start begins polling updates and decision notifications.
func (b *Bot) Start() {
	if b == nil || b.bot == nil {
		return
	}

	update := tgbotapi.NewUpdate(0)
	update.Timeout = 60
	updates := b.bot.GetUpdatesChan(update)

	go b.watchDecisions()

	for upd := range updates {
		if upd.Message == nil {
			continue
		}
		b.handleMessage(upd.Message)
	}
}

func (b *Bot) handleMessage(msg *tgbotapi.Message) {
	if msg == nil {
		return
	}
	chatID := msg.Chat.ID
	command := strings.ToLower(strings.TrimSpace(msg.Command()))
	args := strings.Fields(strings.TrimSpace(msg.CommandArguments()))

	if command == "" {
		return
	}
	logger.Infof("📡 Telegram command received: chat=%d cmd=%s args=%v", chatID, command, args)

	switch command {
	case "start":
		b.reply(chatID, "欢迎使用 NOFX 机器人。\n请先发送 /auth <密码> 完成验证，再使用查询命令。\n输入 /help 查看指令列表。")
		return
	case "help":
		b.reply(chatID, b.helpText())
		return
	case "auth":
		b.handleAuth(chatID, args)
		return
	}

	if !b.isAuthorized(chatID) {
		b.reply(chatID, "未授权，请先发送 /auth <密码> 完成验证。")
		return
	}

	switch command {
	case "status":
		b.handleStatus(chatID, args)
	case "balance":
		b.handleBalance(chatID, args)
	case "positions":
		b.handlePositions(chatID, args)
	case "orders":
		b.handleOrders(chatID, args)
	case "deferred":
		b.handleDeferred(chatID, args)
	case "decision":
		b.handleDecision(chatID, args)
	case "alerts":
		b.handleAlerts(chatID, args)
	case "traders":
		b.handleTraders(chatID)
	default:
		b.reply(chatID, "未知命令，请发送 /help 查看指令列表。")
	}
}

func (b *Bot) handleAuth(chatID int64, args []string) {
	if len(args) == 0 {
		b.reply(chatID, "用法：/auth <密码>")
		return
	}
	raw := strings.TrimSpace(strings.Join(args, " "))
	clean := strings.TrimSpace(raw)
	punctStripped := strings.Trim(clean, ".,。!！")
	if clean != b.authPassword && punctStripped != b.authPassword {
		logger.Infof("📡 Telegram auth failed: chat=%d", chatID)
		b.reply(chatID, "密码错误，请重试。")
		return
	}
	if b.authorizeChat(chatID) {
		logger.Infof("📡 Telegram auth success: chat=%d", chatID)
		b.reply(chatID, "✅ 验证通过，已授权使用机器人。")
	} else {
		b.reply(chatID, "✅ 已授权使用机器人。")
	}
}

func (b *Bot) handleStatus(chatID int64, args []string) {
	at, errMsg := b.resolveTrader(chatID, args)
	if errMsg != "" {
		b.reply(chatID, errMsg)
		return
	}

	status := at.GetStatus()
	lines := []string{
		fmt.Sprintf("交易员：%s", at.GetName()),
		fmt.Sprintf("运行状态：%v", status["is_running"]),
		fmt.Sprintf("交易所：%v", status["exchange"]),
		fmt.Sprintf("AI 模型：%v", status["ai_model"]),
		fmt.Sprintf("运行时长：%v 分钟", status["runtime_minutes"]),
		fmt.Sprintf("扫描周期：%v", status["scan_interval"]),
	}

	if ws, ok := status["user_data_ws"].(trader.UserDataStreamStatus); ok {
		lines = append(lines, fmt.Sprintf("EXCHANGE_WS：%s", strings.ToUpper(ws.State)))
	} else if wsMap, ok := status["user_data_ws"].(map[string]interface{}); ok {
		lines = append(lines, fmt.Sprintf("EXCHANGE_WS：%s", strings.ToUpper(asString(wsMap["state"]))))
	}
	if ws, ok := status["mark_price_ws"].(trader.MarkPriceStreamStatus); ok {
		lines = append(lines, fmt.Sprintf("MARK_PRICE_WS：%s", strings.ToUpper(ws.State)))
	} else if wsMap, ok := status["mark_price_ws"].(map[string]interface{}); ok {
		lines = append(lines, fmt.Sprintf("MARK_PRICE_WS：%s", strings.ToUpper(asString(wsMap["state"]))))
	}

	b.reply(chatID, strings.Join(lines, "\n"))
}

func (b *Bot) handleBalance(chatID int64, args []string) {
	at, errMsg := b.resolveTrader(chatID, args)
	if errMsg != "" {
		b.reply(chatID, errMsg)
		return
	}

	account, ok := at.GetAccountInfoCached()
	if !ok {
		var err error
		account, err = at.GetAccountInfo()
		if err != nil {
			b.reply(chatID, fmt.Sprintf("获取账户失败：%v", err))
			return
		}
	}

	lines := []string{
		fmt.Sprintf("交易员：%s", at.GetName()),
		fmt.Sprintf("权益：%s", formatFloat(account["total_equity"], 2)),
		fmt.Sprintf("可用余额：%s", formatFloat(account["available_balance"], 2)),
		fmt.Sprintf("保证金占用：%s (%s%%)", formatFloat(account["margin_used"], 2), formatFloat(account["margin_used_pct"], 2)),
		fmt.Sprintf("未实现盈亏：%s", formatFloat(account["unrealized_profit"], 2)),
		fmt.Sprintf("总盈亏：%s (%s%%)", formatFloat(account["total_pnl"], 2), formatFloat(account["total_pnl_pct"], 2)),
	}

	b.reply(chatID, strings.Join(lines, "\n"))
}

func (b *Bot) handlePositions(chatID int64, args []string) {
	at, errMsg := b.resolveTrader(chatID, args)
	if errMsg != "" {
		b.reply(chatID, errMsg)
		return
	}

	var positions []map[string]interface{}
	if cached, ok := at.GetPositionsCached(); ok {
		positions = cached
	} else {
		data, err := at.GetPositions()
		if err != nil {
			b.reply(chatID, fmt.Sprintf("获取持仓失败：%v", err))
			return
		}
		positions = data
	}

	if len(positions) == 0 {
		b.reply(chatID, "当前无持仓。")
		return
	}

	lines := []string{fmt.Sprintf("交易员：%s 当前持仓", at.GetName())}
	for _, pos := range positions {
		symbol := asString(pos["symbol"])
		side := strings.ToUpper(asString(pos["side"]))
		qty := formatFloat(pos["quantity"], 6)
		entry := formatFloat(pos["entry_price"], 4)
		mark := formatFloat(pos["mark_price"], 4)
		pnl := formatFloat(pos["unrealized_pnl"], 2)
		pnlPct := formatFloat(pos["unrealized_pnl_pct"], 2)
		lines = append(lines, fmt.Sprintf("%s %s | Qty %s | Entry %s | Mark %s | PnL %s (%s%%)", symbol, side, qty, entry, mark, pnl, pnlPct))
	}

	b.reply(chatID, strings.Join(lines, "\n"))
}

func (b *Bot) handleOrders(chatID int64, args []string) {
	at, errMsg := b.resolveTrader(chatID, args)
	if errMsg != "" {
		b.reply(chatID, errMsg)
		return
	}

	var orders []trader.OpenOrder
	if cached, ok := at.GetOpenOrdersCached(""); ok {
		orders = cached
	} else {
		data, err := at.GetOpenOrders("")
		if err != nil {
			b.reply(chatID, fmt.Sprintf("获取挂单失败：%v", err))
			return
		}
		orders = data
	}

	if len(orders) == 0 {
		b.reply(chatID, "当前无挂单。")
		return
	}

	lines := []string{fmt.Sprintf("交易员：%s 当前挂单", at.GetName())}
	for _, order := range orders {
		price := order.Price
		if order.StopPrice > 0 {
			price = order.StopPrice
		}
		lines = append(lines, fmt.Sprintf("%s %s %s | %s | Qty %.6f | Price %.4f | %s",
			order.Symbol, order.Side, order.PositionSide, order.Type, order.Quantity, price, order.Status))
	}
	b.reply(chatID, strings.Join(lines, "\n"))
}

func (b *Bot) handleDeferred(chatID int64, args []string) {
	at, errMsg := b.resolveTrader(chatID, args)
	if errMsg != "" {
		b.reply(chatID, errMsg)
		return
	}

	orders := at.GetDeferredConditionalOrders("")
	if len(orders) == 0 {
		b.reply(chatID, "当前没有缓存的止盈止损。")
		return
	}
	lines := []string{fmt.Sprintf("交易员：%s 缓存止盈止损", at.GetName())}
	for _, order := range orders {
		lines = append(lines, fmt.Sprintf("%s %s | SL %s | TP %s",
			order.Symbol, order.PositionSide, formatFloat(order.StopLoss, 4), formatFloat(order.TakeProfit, 4)))
	}
	b.reply(chatID, strings.Join(lines, "\n"))
}

func (b *Bot) handleDecision(chatID int64, args []string) {
	at, errMsg := b.resolveTrader(chatID, args)
	if errMsg != "" {
		b.reply(chatID, errMsg)
		return
	}
	if b.store == nil {
		b.reply(chatID, "当前无决策数据。")
		return
	}

	records, err := b.store.Decision().GetLatestRecords(at.GetID(), 1)
	if err != nil || len(records) == 0 {
		b.reply(chatID, "当前无决策记录。")
		return
	}

	rec := records[len(records)-1]
	status := "✅ 成功"
	if !rec.Success {
		status = "❌ 失败"
	}
	lines := []string{
		fmt.Sprintf("交易员：%s", at.GetName()),
		fmt.Sprintf("周期 #%d | %s", rec.CycleNumber, status),
	}
	if rec.ErrorMessage != "" {
		lines = append(lines, fmt.Sprintf("错误：%s", rec.ErrorMessage))
	}
	for _, action := range rec.Decisions {
		flag := "✅"
		if !action.Success {
			flag = "❌"
		}
		lines = append(lines, fmt.Sprintf("%s %s %s", flag, action.Symbol, action.Action))
	}
	b.reply(chatID, strings.Join(lines, "\n"))
}

func (b *Bot) handleAlerts(chatID int64, args []string) {
	at, errMsg := b.resolveTrader(chatID, args)
	if errMsg != "" {
		b.reply(chatID, errMsg)
		return
	}
	if b.store == nil {
		b.reply(chatID, "当前无告警数据。")
		return
	}

	records, err := b.store.Decision().GetLatestRecords(at.GetID(), 20)
	if err != nil || len(records) == 0 {
		b.reply(chatID, "当前无告警记录。")
		return
	}

	var alerts []string
	for i := len(records) - 1; i >= 0; i-- {
		rec := records[i]
		if rec.Success && rec.ErrorMessage == "" {
			continue
		}
		msg := fmt.Sprintf("#%d %s", rec.CycleNumber, rec.ErrorMessage)
		if rec.ErrorMessage == "" {
			msg = fmt.Sprintf("#%d 执行失败", rec.CycleNumber)
		}
		alerts = append(alerts, msg)
		if len(alerts) >= 5 {
			break
		}
	}

	if len(alerts) == 0 {
		b.reply(chatID, "当前无告警记录。")
		return
	}
	lines := append([]string{fmt.Sprintf("交易员：%s 最近告警", at.GetName())}, alerts...)
	b.reply(chatID, strings.Join(lines, "\n"))
}

func (b *Bot) handleTraders(chatID int64) {
	traders := b.manager.GetAllTraders()
	if len(traders) == 0 {
		b.reply(chatID, "当前没有交易员。")
		return
	}
	lines := []string{"交易员列表："}
	ids := make([]string, 0, len(traders))
	for id := range traders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t := traders[id]
		status := t.GetStatus()
		running := false
		if v, ok := status["is_running"].(bool); ok {
			running = v
		}
		state := "停止"
		if running {
			state = "运行中"
		}
		lines = append(lines, fmt.Sprintf("%s | %s | %s", id[:8], t.GetName(), state))
	}
	b.reply(chatID, strings.Join(lines, "\n"))
}

func (b *Bot) resolveTrader(chatID int64, args []string) (*trader.AutoTrader, string) {
	traders := b.manager.GetAllTraders()
	if len(traders) == 0 {
		return nil, "当前没有交易员。"
	}

	var pickedID string
	if len(args) > 0 {
		pickedID = strings.TrimSpace(args[0])
	}

	if pickedID == "" {
		if id := b.getChatTrader(chatID); id != "" {
			if t, ok := traders[id]; ok {
				return t, ""
			}
		}
		if len(traders) == 1 {
			for id, t := range traders {
				b.setChatTrader(chatID, id)
				return t, ""
			}
		}
		return nil, b.traderPickHint(traders)
	}

	if t, ok := traders[pickedID]; ok {
		b.setChatTrader(chatID, pickedID)
		return t, ""
	}

	var matchedID string
	for id, t := range traders {
		if strings.HasPrefix(id, pickedID) {
			matchedID = id
			_ = t
			break
		}
	}
	if matchedID != "" {
		b.setChatTrader(chatID, matchedID)
		return traders[matchedID], ""
	}

	for id, t := range traders {
		if strings.EqualFold(t.GetName(), pickedID) {
			b.setChatTrader(chatID, id)
			return t, ""
		}
	}

	return nil, b.traderPickHint(traders)
}

func (b *Bot) traderPickHint(traders map[string]*trader.AutoTrader) string {
	lines := []string{"请指定交易员 ID 或名称，例如：/status <trader_id>。"}
	ids := make([]string, 0, len(traders))
	for id := range traders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		t := traders[id]
		lines = append(lines, fmt.Sprintf("%s | %s", id[:8], t.GetName()))
	}
	return strings.Join(lines, "\n")
}

func (b *Bot) helpText() string {
	return strings.Join([]string{
		"start - 启动机器人并显示快速帮助",
		"help - 显示命令列表与用法",
		"auth - 使用密码授权（/auth <密码>）",
		"status - 查看交易员状态（可加 trader_id）",
		"balance - 查看权益、余额、保证金与盈亏",
		"positions - 查看当前持仓汇总",
		"orders - 查看当前挂单",
		"deferred - 查看系统内部缓存止盈止损",
		"decision - 查看最新 AI 决策与执行结果",
		"alerts - 查看最近告警与风控触发",
		"traders - 查看交易员列表",
	}, "\n")
}

func (b *Bot) reply(chatID int64, text string) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.DisableWebPagePreview = true
	if _, err := b.bot.Send(msg); err != nil {
		logger.Warnf("⚠️ Telegram send failed: %v", err)
	}
}

func (b *Bot) isAuthorized(chatID int64) bool {
	b.authMu.RLock()
	defer b.authMu.RUnlock()
	_, ok := b.authChats[chatID]
	return ok
}

func (b *Bot) authorizeChat(chatID int64) bool {
	b.authMu.Lock()
	_, exists := b.authChats[chatID]
	if !exists {
		b.authChats[chatID] = struct{}{}
	}
	b.authMu.Unlock()

	if !exists {
		go b.saveAuthorizedChats()
	}
	return !exists
}

func (b *Bot) loadAuthorizedChats() {
	if b.store == nil {
		return
	}
	raw, err := b.store.GetSystemConfig(authorizedChatsKey)
	if err != nil || raw == "" {
		return
	}
	var ids []int64
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return
	}
	b.authMu.Lock()
	for _, id := range ids {
		b.authChats[id] = struct{}{}
	}
	b.authMu.Unlock()
}

func (b *Bot) saveAuthorizedChats() {
	if b.store == nil {
		return
	}
	b.authMu.RLock()
	ids := make([]int64, 0, len(b.authChats))
	for id := range b.authChats {
		ids = append(ids, id)
	}
	b.authMu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	payload, _ := json.Marshal(ids)
	if err := b.store.SetSystemConfig(authorizedChatsKey, string(payload)); err != nil {
		logger.Warnf("⚠️ Failed to persist Telegram authorized chats: %v", err)
	}
}

func (b *Bot) getChatTrader(chatID int64) string {
	b.chatTraderMu.RLock()
	defer b.chatTraderMu.RUnlock()
	return b.chatTrader[chatID]
}

func (b *Bot) setChatTrader(chatID int64, traderID string) {
	b.chatTraderMu.Lock()
	b.chatTrader[chatID] = traderID
	b.chatTraderMu.Unlock()
}

func (b *Bot) initDecisionCursors() {
	if b.store == nil {
		return
	}
	for _, id := range b.manager.GetTraderIDs() {
		records, err := b.store.Decision().GetLatestRecords(id, 1)
		if err != nil || len(records) == 0 {
			continue
		}
		b.decisionAfter[id] = records[len(records)-1].ID
	}
}

func (b *Bot) watchDecisions() {
	if b.store == nil {
		return
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		chatIDs := b.getAuthorizedChatIDs()
		if len(chatIDs) == 0 {
			continue
		}
		traders := b.manager.GetAllTraders()
		for id, t := range traders {
			lastID := b.getDecisionCursor(id)
			records, err := b.store.Decision().GetRecordsAfterID(id, lastID, 20)
			if err != nil || len(records) == 0 {
				continue
			}
			for _, rec := range records {
				if b.shouldSkipDecisionNotify(rec) {
					b.setDecisionCursor(id, rec.ID)
					continue
				}
				msg := b.formatDecisionNotify(t.GetName(), rec)
				for _, chatID := range chatIDs {
					b.reply(chatID, msg)
				}
				b.setDecisionCursor(id, rec.ID)
			}
		}
	}
}

func (b *Bot) shouldSkipDecisionNotify(rec *store.DecisionRecord) bool {
	if rec == nil {
		return true
	}
	if len(rec.Decisions) == 0 && rec.ErrorMessage == "" {
		return true
	}
	return false
}

func (b *Bot) formatDecisionNotify(traderName string, rec *store.DecisionRecord) string {
	status := "✅"
	if rec != nil && !rec.Success {
		status = "❌"
	}
	lines := []string{
		fmt.Sprintf("%s %s 决策 #%d", status, traderName, rec.CycleNumber),
	}
	if rec.ErrorMessage != "" {
		lines = append(lines, fmt.Sprintf("错误：%s", rec.ErrorMessage))
	}
	for _, action := range rec.Decisions {
		flag := "✅"
		if !action.Success {
			flag = "❌"
		}
		lines = append(lines, fmt.Sprintf("%s %s %s", flag, action.Symbol, action.Action))
	}
	return strings.Join(lines, "\n")
}

func (b *Bot) getAuthorizedChatIDs() []int64 {
	b.authMu.RLock()
	defer b.authMu.RUnlock()
	ids := make([]int64, 0, len(b.authChats))
	for id := range b.authChats {
		ids = append(ids, id)
	}
	return ids
}

func (b *Bot) getDecisionCursor(traderID string) int64 {
	b.decisionMu.Lock()
	defer b.decisionMu.Unlock()
	return b.decisionAfter[traderID]
}

func (b *Bot) setDecisionCursor(traderID string, id int64) {
	b.decisionMu.Lock()
	b.decisionAfter[traderID] = id
	b.decisionMu.Unlock()
}

func formatFloat(value interface{}, precision int) string {
	switch v := value.(type) {
	case float64:
		return fmt.Sprintf("%.*f", precision, v)
	case float32:
		return fmt.Sprintf("%.*f", precision, v)
	case int:
		return fmt.Sprintf("%.*f", precision, float64(v))
	case int64:
		return fmt.Sprintf("%.*f", precision, float64(v))
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return v
		}
		return fmt.Sprintf("%.*f", precision, parsed)
	default:
		return "0"
	}
}

func asString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case fmt.Stringer:
		return v.String()
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return fmt.Sprintf("%.4f", v)
	default:
		return ""
	}
}
