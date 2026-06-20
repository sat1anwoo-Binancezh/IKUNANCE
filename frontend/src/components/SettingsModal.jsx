import React from 'react'
import { beepSound } from '../hooks/useAudio.js'

export default function SettingsModal({
  modalTab,
  setModalTab,
  onClose,
  onSave,
  cfg,
  setCfg,
  emailTemplate,
  setEmailTemplate,
  customSounds,
  soundName,
  setSoundName,
  soundFileRef,
  uploadSound,
  deleteSound,
  sendTestEmail,
  testEmailLoading,
  testEmailResult,
}) {
  const updateCfg = (key, value) => setCfg(prev => ({ ...prev, [key]: value }))
  const updateTemplate = (key, value) => setEmailTemplate(prev => ({ ...prev, [key]: value }))

  return (
    <div className="modal" onClick={e => e.target === e.currentTarget && onClose?.()}>
      <div className="modal-container">
        <div className="modal-tabs">
          {[
            ['api', 'API'],
            ['email', '邮件'],
            ['sound', '音效'],
            ['ai', 'AI'],
          ].map(([key, label]) => (
            <button key={key} className={modalTab === key ? 'active' : ''} onClick={() => setModalTab(key)}>{label}</button>
          ))}
        </div>
        <div className="modal-body">
          {modalTab === 'api' && (
            <div className="settings-form">
              <label>API Key<input value={cfg.apiKey || ''} onChange={e => updateCfg('apiKey', e.target.value)} /></label>
              <label>Secret Key<input type="password" value={cfg.secretKey || ''} onChange={e => updateCfg('secretKey', e.target.value)} /></label>
              <label>Proxy<input value={cfg.proxy || ''} onChange={e => updateCfg('proxy', e.target.value)} /></label>
            </div>
          )}
          {modalTab === 'email' && (
            <div className="settings-form">
              <label>Email<input value={cfg.email || ''} onChange={e => updateCfg('email', e.target.value)} /></label>
              <label>授权码<input type="password" value={cfg.emailPass || ''} onChange={e => updateCfg('emailPass', e.target.value)} /></label>
              <div className="checkbox-grid">
                {Object.keys(emailTemplate || {}).map(key => (
                  <label key={key}><input type="checkbox" checked={!!emailTemplate[key]} onChange={e => updateTemplate(key, e.target.checked)} /> {key}</label>
                ))}
              </div>
              <button onClick={sendTestEmail} disabled={testEmailLoading}>{testEmailLoading ? '测试中...' : '发送测试邮件'}</button>
              {testEmailResult && <div className="settings-result">{testEmailResult}</div>}
            </div>
          )}
          {modalTab === 'sound' && (
            <div className="settings-form">
              <label>音效名称<input value={soundName || ''} onChange={e => setSoundName(e.target.value)} /></label>
              <input ref={soundFileRef} type="file" accept="audio/mpeg,.mp3" />
              <button onClick={uploadSound}>上传音效</button>
              <button onClick={beepSound}>播放默认音</button>
              <div className="sound-list">
                {(customSounds || []).map(item => (
                  <div key={item.file || item.name} className="sound-row">
                    <span>{item.name || item.file}</span>
                    <button onClick={() => deleteSound(item.file)}>删除</button>
                  </div>
                ))}
              </div>
            </div>
          )}
          {modalTab === 'ai' && (
            <div className="settings-form">
              <label>豆包 API Key<input type="password" value={cfg.doubaoApiKey || ''} onChange={e => updateCfg('doubaoApiKey', e.target.value)} /></label>
            </div>
          )}
        </div>
        <div className="modal-actions">
          <button onClick={onClose}>取消</button>
          <button onClick={onSave}>保存</button>
        </div>
      </div>
    </div>
  )
}
