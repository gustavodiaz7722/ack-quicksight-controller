{{/* 
  This hook is called after setting the output fields from the UpdateDataSource API call.
  It sets the Status field from UpdateStatus to reflect the current state after update.
*/}}
if resp.UpdateStatus != "" {
    ko.Status.Status = aws.String(string(resp.UpdateStatus))
}
