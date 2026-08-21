package serviceconfig

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// ReloadClass says whether a field can take effect without restarting the
// process.
type ReloadClass string

const (
	// ReloadLive fields are re-read on the next use, so a reload applies them.
	ReloadLive ReloadClass = "live"
	// ReloadRestart fields are consumed once during startup. A bound socket, an
	// open client connection, or an identity already published cannot be
	// changed underneath a running process.
	ReloadRestart ReloadClass = "restart"
)

// FieldPolicy classifies one config field.
type FieldPolicy struct {
	Class ReloadClass
	// Why explains a restart classification. An operator reading a rejected
	// reload needs to know what makes the field different, not just that it is.
	Why string
}

// reloadPolicies is the per-process classification.
//
// A path entry covers everything beneath it. The default for an unlisted field
// is deliberately absent rather than "live": TestEveryFieldIsClassified fails on
// a field nobody classified, because guessing live for an unknown field is how
// a reload silently does nothing.
var reloadPolicies = map[string]map[string]FieldPolicy{
	ProcessGateway: {
		"gateway.gateway_id":                      {ReloadRestart, "the gateway identity is already published to the fleet registry and to metadata"},
		"gateway.listen":                          {ReloadRestart, "the socket is already bound"},
		"gateway.data_listen":                     {ReloadRestart, "the socket is already bound"},
		"gateway.advertise_control_address":       {ReloadRestart, "the advertised address is already published"},
		"gateway.advertise_data_address":          {ReloadRestart, "the advertised address is already published"},
		"gateway.data_disable":                    {ReloadRestart, "enabling or disabling the dataplane listener changes what is bound"},
		"gateway.tls":                             {ReloadRestart, "the TLS material is loaded into the running listener"},
		"gateway.etcd":                            {ReloadRestart, "the etcd client is already connected"},
		"gateway.sbs_admin_endpoint":              {ReloadRestart, "the admin client is already connected"},
		"gateway.metadata_backend":                {ReloadRestart, "the metadata backend is chosen once at startup"},
		"gateway.data_backend_mode":               {ReloadRestart, "the data backend is chosen once at startup"},
		"gateway.cache":                           {ReloadLive, ""},
		"gateway.dependency":                      {ReloadLive, ""},
		"gateway.reconcile":                       {ReloadLive, ""},
		"gateway.dataplane.max_inflight_requests": {ReloadLive, ""},
		"gateway.dataplane.max_inflight_bytes":    {ReloadLive, ""},
		"gateway.dataplane.max_io_size":           {ReloadLive, ""},
		"gateway.dataplane.token_ttl_seconds":     {ReloadLive, ""},
		"gateway.dataplane.token_key":             {ReloadRestart, "rotating token material under live sessions would invalidate tokens already issued"},
		"gateway.dataplane.session_key":           {ReloadRestart, "rotating session material under live sessions would break established sessions"},
		"gateway.dataplane.wire_version":          {ReloadRestart, "the wire version is negotiated when a session opens"},
		"gateway.observability":                   {ReloadLive, ""},
	},
	ProcessISCSIGateway: {
		"iscsi_gateway.gateway_id":                  {ReloadRestart, "the gateway identity is already published to the fleet registry"},
		"iscsi_gateway.advertise_portals":           {ReloadRestart, "the portal is already bound and advertised"},
		"iscsi_gateway.etcd":                        {ReloadRestart, "the etcd client is already connected"},
		"iscsi_gateway.sbs_endpoint":                {ReloadRestart, "the SBS client is already connected"},
		"iscsi_gateway.sbs_admin_endpoint":          {ReloadRestart, "the admin client is already connected"},
		"iscsi_gateway.sbs_endpoint_tls":            {ReloadRestart, "the TLS material is loaded into the running client"},
		"iscsi_gateway.auth.mode":                   {ReloadRestart, "changing the auth mode under established sessions would leave them on the old contract"},
		"iscsi_gateway.auth.chap_secret":            {ReloadRestart, "rotating CHAP material under established sessions would break them"},
		"iscsi_gateway.auth.allowed_initiator_iqns": {ReloadLive, ""},
		"iscsi_gateway.dependency":                  {ReloadLive, ""},
		"iscsi_gateway.reload":                      {ReloadLive, ""},
		"iscsi_gateway.observability":               {ReloadLive, ""},
	},
	ProcessSBSService: {
		"sbs_service.cluster_id":           {ReloadRestart, "cluster identity is used to key every metadata record this service writes"},
		"sbs_service.sbs_cluster_id":       {ReloadRestart, "cluster identity is used to key every metadata record this service writes"},
		"sbs_service.node_id":              {ReloadRestart, "the node identity holds the leader lease"},
		"sbs_service.metadata_backend":     {ReloadRestart, "the metadata authority is opened once at startup"},
		"sbs_service.grpc_listen":          {ReloadRestart, "the socket is already bound"},
		"sbs_service.http_listen":          {ReloadRestart, "the socket is already bound"},
		"sbs_service.payload_root":         {ReloadRestart, "the payload root is opened at startup"},
		"sbs_service.tikv.pd_endpoints":    {ReloadRestart, "the TiKV client is already connected"},
		"sbs_service.tikv.keyspace":        {ReloadRestart, "the keyspace selects which records this service reads"},
		"sbs_service.tikv.api_version":     {ReloadRestart, "the API version is negotiated when the client connects"},
		"sbs_service.tikv.tls":             {ReloadRestart, "the TLS material is loaded into the running client"},
		"sbs_service.tikv.timeout_seconds": {ReloadLive, ""},
		"sbs_service.tikv.scan_page_size":  {ReloadLive, ""},
		"sbs_service.tikv.batch_get_size":  {ReloadLive, ""},
		"sbs_service.tikv.operation_trace": {ReloadLive, ""},
		// The leader lease is held with the current timings. Changing them under
		// a held lease risks a renewal that arrives after the old expiry.
		"sbs_service.leader":        {ReloadRestart, "the leader lease is already held with the current timings"},
		"sbs_service.dependency":    {ReloadLive, ""},
		"sbs_service.health":        {ReloadLive, ""},
		"sbs_service.write_effects": {ReloadRestart, "the write-effects queue is running with the current profile"},
		"sbs_service.observability": {ReloadLive, ""},
	},
	ProcessSBSData: {
		"sbs_data.cluster_id":        {ReloadRestart, "cluster identity scopes the membership and payload evidence"},
		"sbs_data.sbs_cluster_id":    {ReloadRestart, "SBS cluster identity scopes the membership and payload evidence"},
		"sbs_data.node_id":           {ReloadRestart, "the node identity is registered with sbs-service"},
		"sbs_data.data_path":         {ReloadRestart, "the payload store is opened at startup"},
		"sbs_data.grpc_listen":       {ReloadRestart, "the socket is already bound"},
		"sbs_data.http_listen":       {ReloadRestart, "the socket is already bound"},
		"sbs_data.store_config_path": {ReloadLive, ""},
		"sbs_data.observability":     {ReloadLive, ""},
	},
	ProcessCSIDriver: {
		"csi_driver.driver_name":     {ReloadRestart, "the driver name is registered with the kubelet"},
		"csi_driver.node_id":         {ReloadRestart, "the node identity is registered with the kubelet"},
		"csi_driver.endpoint":        {ReloadRestart, "the CSI socket is already bound"},
		"csi_driver.admin_endpoints": {ReloadRestart, "the admin client is already connected"},
		"csi_driver.cluster_id":      {ReloadRestart, "cluster identity is used to address every volume this driver manages"},
		"csi_driver.sbs_cluster_id":  {ReloadRestart, "cluster identity is used to address every volume this driver manages"},
		"csi_driver.gateway_url":     {ReloadLive, ""},
		"csi_driver.observability":   {ReloadLive, ""},
	},
	ProcessMCP: {
		"mcp.operations_endpoint":  {ReloadLive, ""},
		"mcp.mode":                 {ReloadRestart, "the posture is asserted to clients when the stdio session opens"},
		"mcp.approval_policy":      {ReloadRestart, "the approval contract is asserted to clients when the stdio session opens"},
		"mcp.operation_output_dir": {ReloadLive, ""},
		"mcp.http_timeout_seconds": {ReloadLive, ""},
		"mcp.observability":        {ReloadLive, ""},
	},
}

// Top-level fields are restart-class for every process: they identify which
// contract the file speaks, and a reload cannot change that underneath a
// running process.
var topLevelPolicy = map[string]FieldPolicy{
	"schema_version": {ReloadRestart, "the schema version selects which contract the process is running"},
	"process":        {ReloadRestart, "a config cannot change which process it configures"},
	"profile":        {ReloadRestart, "the profile decides which settings are admissible at all"},
	"revision":       {ReloadLive, ""},
}

// ReloadPolicyFor returns the classification for a process.
func ReloadPolicyFor(process string) map[string]FieldPolicy {
	out := map[string]FieldPolicy{}
	for k, v := range topLevelPolicy {
		out[k] = v
	}
	for k, v := range reloadPolicies[process] {
		out[k] = v
	}
	return out
}

// classify finds the most specific policy covering a field path.
func classify(policy map[string]FieldPolicy, path string) (FieldPolicy, bool) {
	best := ""
	for prefix := range policy {
		if path == prefix || strings.HasPrefix(path, prefix+".") {
			if len(prefix) > len(best) {
				best = prefix
			}
		}
	}
	if best == "" {
		return FieldPolicy{}, false
	}
	return policy[best], true
}

// ReloadResult records what a reload attempt did.
type ReloadResult struct {
	Applied         []string `json:"config_reload_applied_fields"`
	RestartRequired []string `json:"config_reload_restart_required_fields"`
	FromRevision    int      `json:"config_reload_from_revision"`
	ToRevision      int      `json:"config_reload_to_revision"`
	Accepted        bool     `json:"config_reload_accepted"`
	// RollbackNote states what is in effect after this attempt, which is the
	// thing an operator actually needs after a rejection.
	RollbackNote string   `json:"config_reload_rollback_note"`
	Errors       []string `json:"config_reload_errors,omitempty"`
}

// Reload compares a candidate config against the running one and decides
// whether it can be applied without a restart.
//
// A reload is all or nothing. If any restart-class field changed the whole
// reload is rejected, including the live fields that could have been applied.
// Applying part of a file would leave the process matching neither the old
// configuration nor the new one, which makes the revision an operator reads
// meaningless, and the revision is the only handle they have on a fleet.
func Reload(current, next *File, process string) ReloadResult {
	res := ReloadResult{RollbackNote: "the previously loaded configuration remains in effect"}
	if current == nil || next == nil {
		res.Errors = append(res.Errors, "reload needs both the running and the candidate configuration")
		return res
	}
	res.FromRevision, res.ToRevision = current.Revision, next.Revision

	if vr := Validate(next); !vr.OK() {
		res.Errors = append(res.Errors, vr.Errors...)
		return res
	}
	if next.Process != current.Process {
		res.Errors = append(res.Errors,
			fmt.Sprintf("candidate configures %q but this process is %q", next.Process, current.Process))
		return res
	}
	if next.Revision == current.Revision && !hasAnyChange(current, next) {
		res.Accepted = true
		res.RollbackNote = "no change; the running configuration is unchanged"
		return res
	}

	policy := ReloadPolicyFor(process)
	changed := changedPaths(reflect.ValueOf(*current), reflect.ValueOf(*next), "")
	var unclassified []string
	for _, path := range changed {
		p, ok := classify(policy, path)
		if !ok {
			unclassified = append(unclassified, path)
			continue
		}
		if p.Class == ReloadRestart {
			res.RestartRequired = append(res.RestartRequired,
				fmt.Sprintf("%s: %s", path, p.Why))
			continue
		}
		res.Applied = append(res.Applied, path)
	}
	sort.Strings(res.Applied)
	sort.Strings(res.RestartRequired)

	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		res.Errors = append(res.Errors,
			fmt.Sprintf("no reload classification for %s; refusing rather than guessing whether a restart is needed",
				strings.Join(unclassified, ", ")))
		res.Applied = nil
		return res
	}
	if len(res.RestartRequired) > 0 {
		res.Errors = append(res.Errors,
			fmt.Sprintf("%d field(s) require a process restart; no field was applied", len(res.RestartRequired)))
		res.Applied = nil
		return res
	}
	res.Accepted = true
	res.RollbackNote = "the candidate configuration is now in effect; reverting means reloading the previous file"
	return res
}

func hasAnyChange(a, b *File) bool {
	return len(changedPaths(reflect.ValueOf(*a), reflect.ValueOf(*b), "")) > 0
}

// changedPaths walks two configs and returns the dotted yaml paths that differ.
func changedPaths(a, b reflect.Value, prefix string) []string {
	var out []string
	for a.Kind() == reflect.Ptr {
		if a.IsNil() || b.IsNil() {
			if !reflect.DeepEqual(a.Interface(), b.Interface()) && prefix != "" {
				return []string{prefix}
			}
			return nil
		}
		a, b = a.Elem(), b.Elem()
	}
	if a.Kind() != reflect.Struct {
		if !reflect.DeepEqual(a.Interface(), b.Interface()) && prefix != "" {
			return []string{prefix}
		}
		return nil
	}
	t := a.Type()
	for i := 0; i < t.NumField(); i++ {
		tag := strings.Split(t.Field(i).Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		path := tag
		if prefix != "" {
			path = prefix + "." + tag
		}
		out = append(out, changedPaths(a.Field(i), b.Field(i), path)...)
	}
	return out
}

// FieldPaths returns every dotted yaml leaf path for a config type, used by
// tests that check the policy covers everything.
func FieldPaths(t reflect.Type, prefix string) []string {
	var out []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		path := tag
		if prefix != "" {
			path = prefix + "." + tag
		}
		ft := f.Type
		for ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct {
			out = append(out, FieldPaths(ft, path)...)
			continue
		}
		out = append(out, path)
	}
	return out
}
