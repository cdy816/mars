using System;
using System.Threading.Tasks;
using Microsoft.JSInterop;

namespace DBWebStudio
{
	public class JSModuleInterop : IDisposable
	{
		private readonly Task<IJSObjectReference> moduleTask;
		private IJSObjectReference? module;

		public JSModuleInterop(IJSRuntime js, string filename)
		{
			moduleTask = js.InvokeAsync<IJSObjectReference>("import", filename).AsTask();
		}

		public async Task ImportAsync()
		{
			module = await moduleTask;
		}

		public void Dispose()
		{
			OnDisposingModule();
			Module.DisposeAsync();
		}

		protected IJSObjectReference Module =>
			module ?? throw new InvalidOperationException("Make sure to run ImportAsync() first.");

		protected void Invoke(string identifier, params object?[]? args) =>
			Module.InvokeVoidAsync(identifier, args);

		protected async Task<TValue> Invoke<TValue>(string identifier, params object?[]? args) =>
			await Module.InvokeAsync<TValue>(identifier, args);

		protected virtual void OnDisposingModule() { }
	}
}
