(function(){const t=document.createElement("link").relList;if(t&&t.supports&&t.supports("modulepreload"))return;for(const i of document.querySelectorAll('link[rel="modulepreload"]'))o(i);new MutationObserver(i=>{for(const a of i)if(a.type==="childList")for(const s of a.addedNodes)s.tagName==="LINK"&&s.rel==="modulepreload"&&o(s)}).observe(document,{childList:!0,subtree:!0});function n(i){const a={};return i.integrity&&(a.integrity=i.integrity),i.referrerPolicy&&(a.referrerPolicy=i.referrerPolicy),i.crossOrigin==="use-credentials"?a.credentials="include":i.crossOrigin==="anonymous"?a.credentials="omit":a.credentials="same-origin",a}function o(i){if(i.ep)return;i.ep=!0;const a=n(i);fetch(i.href,a)}})();function np(e){return e&&e.__esModule&&Object.prototype.hasOwnProperty.call(e,"default")?e.default:e}var mc={exports:{}},Vo={},gc={exports:{}},H={};/**
 * @license React
 * react.production.min.js
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var Ir=Symbol.for("react.element"),rp=Symbol.for("react.portal"),op=Symbol.for("react.fragment"),ip=Symbol.for("react.strict_mode"),ap=Symbol.for("react.profiler"),sp=Symbol.for("react.provider"),lp=Symbol.for("react.context"),cp=Symbol.for("react.forward_ref"),dp=Symbol.for("react.suspense"),up=Symbol.for("react.memo"),pp=Symbol.for("react.lazy"),Ks=Symbol.iterator;function fp(e){return e===null||typeof e!="object"?null:(e=Ks&&e[Ks]||e["@@iterator"],typeof e=="function"?e:null)}var hc={isMounted:function(){return!1},enqueueForceUpdate:function(){},enqueueReplaceState:function(){},enqueueSetState:function(){}},xc=Object.assign,vc={};function Bn(e,t,n){this.props=e,this.context=t,this.refs=vc,this.updater=n||hc}Bn.prototype.isReactComponent={};Bn.prototype.setState=function(e,t){if(typeof e!="object"&&typeof e!="function"&&e!=null)throw Error("setState(...): takes an object of state variables to update or a function which returns an object of state variables.");this.updater.enqueueSetState(this,e,t,"setState")};Bn.prototype.forceUpdate=function(e){this.updater.enqueueForceUpdate(this,e,"forceUpdate")};function yc(){}yc.prototype=Bn.prototype;function Ma(e,t,n){this.props=e,this.context=t,this.refs=vc,this.updater=n||hc}var Aa=Ma.prototype=new yc;Aa.constructor=Ma;xc(Aa,Bn.prototype);Aa.isPureReactComponent=!0;var Vs=Array.isArray,bc=Object.prototype.hasOwnProperty,Oa={current:null},kc={key:!0,ref:!0,__self:!0,__source:!0};function jc(e,t,n){var o,i={},a=null,s=null;if(t!=null)for(o in t.ref!==void 0&&(s=t.ref),t.key!==void 0&&(a=""+t.key),t)bc.call(t,o)&&!kc.hasOwnProperty(o)&&(i[o]=t[o]);var l=arguments.length-2;if(l===1)i.children=n;else if(1<l){for(var c=Array(l),d=0;d<l;d++)c[d]=arguments[d+2];i.children=c}if(e&&e.defaultProps)for(o in l=e.defaultProps,l)i[o]===void 0&&(i[o]=l[o]);return{$$typeof:Ir,type:e,key:a,ref:s,props:i,_owner:Oa.current}}function mp(e,t){return{$$typeof:Ir,type:e.type,key:t,ref:e.ref,props:e.props,_owner:e._owner}}function Ba(e){return typeof e=="object"&&e!==null&&e.$$typeof===Ir}function gp(e){var t={"=":"=0",":":"=2"};return"$"+e.replace(/[=:]/g,function(n){return t[n]})}var Gs=/\/+/g;function fi(e,t){return typeof e=="object"&&e!==null&&e.key!=null?gp(""+e.key):t.toString(36)}function lo(e,t,n,o,i){var a=typeof e;(a==="undefined"||a==="boolean")&&(e=null);var s=!1;if(e===null)s=!0;else switch(a){case"string":case"number":s=!0;break;case"object":switch(e.$$typeof){case Ir:case rp:s=!0}}if(s)return s=e,i=i(s),e=o===""?"."+fi(s,0):o,Vs(i)?(n="",e!=null&&(n=e.replace(Gs,"$&/")+"/"),lo(i,t,n,"",function(d){return d})):i!=null&&(Ba(i)&&(i=mp(i,n+(!i.key||s&&s.key===i.key?"":(""+i.key).replace(Gs,"$&/")+"/")+e)),t.push(i)),1;if(s=0,o=o===""?".":o+":",Vs(e))for(var l=0;l<e.length;l++){a=e[l];var c=o+fi(a,l);s+=lo(a,t,n,c,i)}else if(c=fp(e),typeof c=="function")for(e=c.call(e),l=0;!(a=e.next()).done;)a=a.value,c=o+fi(a,l++),s+=lo(a,t,n,c,i);else if(a==="object")throw t=String(e),Error("Objects are not valid as a React child (found: "+(t==="[object Object]"?"object with keys {"+Object.keys(e).join(", ")+"}":t)+"). If you meant to render a collection of children, use an array instead.");return s}function Ur(e,t,n){if(e==null)return e;var o=[],i=0;return lo(e,o,"","",function(a){return t.call(n,a,i++)}),o}function hp(e){if(e._status===-1){var t=e._result;t=t(),t.then(function(n){(e._status===0||e._status===-1)&&(e._status=1,e._result=n)},function(n){(e._status===0||e._status===-1)&&(e._status=2,e._result=n)}),e._status===-1&&(e._status=0,e._result=t)}if(e._status===1)return e._result.default;throw e._result}var Re={current:null},co={transition:null},xp={ReactCurrentDispatcher:Re,ReactCurrentBatchConfig:co,ReactCurrentOwner:Oa};function wc(){throw Error("act(...) is not supported in production builds of React.")}H.Children={map:Ur,forEach:function(e,t,n){Ur(e,function(){t.apply(this,arguments)},n)},count:function(e){var t=0;return Ur(e,function(){t++}),t},toArray:function(e){return Ur(e,function(t){return t})||[]},only:function(e){if(!Ba(e))throw Error("React.Children.only expected to receive a single React element child.");return e}};H.Component=Bn;H.Fragment=op;H.Profiler=ap;H.PureComponent=Ma;H.StrictMode=ip;H.Suspense=dp;H.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED=xp;H.act=wc;H.cloneElement=function(e,t,n){if(e==null)throw Error("React.cloneElement(...): The argument must be a React element, but you passed "+e+".");var o=xc({},e.props),i=e.key,a=e.ref,s=e._owner;if(t!=null){if(t.ref!==void 0&&(a=t.ref,s=Oa.current),t.key!==void 0&&(i=""+t.key),e.type&&e.type.defaultProps)var l=e.type.defaultProps;for(c in t)bc.call(t,c)&&!kc.hasOwnProperty(c)&&(o[c]=t[c]===void 0&&l!==void 0?l[c]:t[c])}var c=arguments.length-2;if(c===1)o.children=n;else if(1<c){l=Array(c);for(var d=0;d<c;d++)l[d]=arguments[d+2];o.children=l}return{$$typeof:Ir,type:e.type,key:i,ref:a,props:o,_owner:s}};H.createContext=function(e){return e={$$typeof:lp,_currentValue:e,_currentValue2:e,_threadCount:0,Provider:null,Consumer:null,_defaultValue:null,_globalName:null},e.Provider={$$typeof:sp,_context:e},e.Consumer=e};H.createElement=jc;H.createFactory=function(e){var t=jc.bind(null,e);return t.type=e,t};H.createRef=function(){return{current:null}};H.forwardRef=function(e){return{$$typeof:cp,render:e}};H.isValidElement=Ba;H.lazy=function(e){return{$$typeof:pp,_payload:{_status:-1,_result:e},_init:hp}};H.memo=function(e,t){return{$$typeof:up,type:e,compare:t===void 0?null:t}};H.startTransition=function(e){var t=co.transition;co.transition={};try{e()}finally{co.transition=t}};H.unstable_act=wc;H.useCallback=function(e,t){return Re.current.useCallback(e,t)};H.useContext=function(e){return Re.current.useContext(e)};H.useDebugValue=function(){};H.useDeferredValue=function(e){return Re.current.useDeferredValue(e)};H.useEffect=function(e,t){return Re.current.useEffect(e,t)};H.useId=function(){return Re.current.useId()};H.useImperativeHandle=function(e,t,n){return Re.current.useImperativeHandle(e,t,n)};H.useInsertionEffect=function(e,t){return Re.current.useInsertionEffect(e,t)};H.useLayoutEffect=function(e,t){return Re.current.useLayoutEffect(e,t)};H.useMemo=function(e,t){return Re.current.useMemo(e,t)};H.useReducer=function(e,t,n){return Re.current.useReducer(e,t,n)};H.useRef=function(e){return Re.current.useRef(e)};H.useState=function(e){return Re.current.useState(e)};H.useSyncExternalStore=function(e,t,n){return Re.current.useSyncExternalStore(e,t,n)};H.useTransition=function(){return Re.current.useTransition()};H.version="18.3.1";gc.exports=H;var x=gc.exports;const lr=np(x);/**
 * @license React
 * react-jsx-runtime.production.min.js
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var vp=x,yp=Symbol.for("react.element"),bp=Symbol.for("react.fragment"),kp=Object.prototype.hasOwnProperty,jp=vp.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED.ReactCurrentOwner,wp={key:!0,ref:!0,__self:!0,__source:!0};function Sc(e,t,n){var o,i={},a=null,s=null;n!==void 0&&(a=""+n),t.key!==void 0&&(a=""+t.key),t.ref!==void 0&&(s=t.ref);for(o in t)kp.call(t,o)&&!wp.hasOwnProperty(o)&&(i[o]=t[o]);if(e&&e.defaultProps)for(o in t=e.defaultProps,t)i[o]===void 0&&(i[o]=t[o]);return{$$typeof:yp,type:e,key:a,ref:s,props:i,_owner:jp.current}}Vo.Fragment=bp;Vo.jsx=Sc;Vo.jsxs=Sc;mc.exports=Vo;var r=mc.exports,Wi={},Nc={exports:{}},Ke={},Cc={exports:{}},Tc={};/**
 * @license React
 * scheduler.production.min.js
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */(function(e){function t(S,R){var M=S.length;S.push(R);e:for(;0<M;){var U=M-1>>>1,D=S[U];if(0<i(D,R))S[U]=R,S[M]=D,M=U;else break e}}function n(S){return S.length===0?null:S[0]}function o(S){if(S.length===0)return null;var R=S[0],M=S.pop();if(M!==R){S[0]=M;e:for(var U=0,D=S.length,$=D>>>1;U<$;){var F=2*(U+1)-1,W=S[F],Y=F+1,te=S[Y];if(0>i(W,M))Y<D&&0>i(te,W)?(S[U]=te,S[Y]=M,U=Y):(S[U]=W,S[F]=M,U=F);else if(Y<D&&0>i(te,M))S[U]=te,S[Y]=M,U=Y;else break e}}return R}function i(S,R){var M=S.sortIndex-R.sortIndex;return M!==0?M:S.id-R.id}if(typeof performance=="object"&&typeof performance.now=="function"){var a=performance;e.unstable_now=function(){return a.now()}}else{var s=Date,l=s.now();e.unstable_now=function(){return s.now()-l}}var c=[],d=[],h=1,f=null,g=3,y=!1,v=!1,b=!1,z=typeof setTimeout=="function"?setTimeout:null,p=typeof clearTimeout=="function"?clearTimeout:null,u=typeof setImmediate<"u"?setImmediate:null;typeof navigator<"u"&&navigator.scheduling!==void 0&&navigator.scheduling.isInputPending!==void 0&&navigator.scheduling.isInputPending.bind(navigator.scheduling);function m(S){for(var R=n(d);R!==null;){if(R.callback===null)o(d);else if(R.startTime<=S)o(d),R.sortIndex=R.expirationTime,t(c,R);else break;R=n(d)}}function j(S){if(b=!1,m(S),!v)if(n(c)!==null)v=!0,pe(T);else{var R=n(d);R!==null&&I(j,R.startTime-S)}}function T(S,R){v=!1,b&&(b=!1,p(_),_=-1),y=!0;var M=g;try{for(m(R),f=n(c);f!==null&&(!(f.expirationTime>R)||S&&!B());){var U=f.callback;if(typeof U=="function"){f.callback=null,g=f.priorityLevel;var D=U(f.expirationTime<=R);R=e.unstable_now(),typeof D=="function"?f.callback=D:f===n(c)&&o(c),m(R)}else o(c);f=n(c)}if(f!==null)var $=!0;else{var F=n(d);F!==null&&I(j,F.startTime-R),$=!1}return $}finally{f=null,g=M,y=!1}}var w=!1,C=null,_=-1,P=5,N=-1;function B(){return!(e.unstable_now()-N<P)}function K(){if(C!==null){var S=e.unstable_now();N=S;var R=!0;try{R=C(!0,S)}finally{R?le():(w=!1,C=null)}}else w=!1}var le;if(typeof u=="function")le=function(){u(K)};else if(typeof MessageChannel<"u"){var Q=new MessageChannel,ye=Q.port2;Q.port1.onmessage=K,le=function(){ye.postMessage(null)}}else le=function(){z(K,0)};function pe(S){C=S,w||(w=!0,le())}function I(S,R){_=z(function(){S(e.unstable_now())},R)}e.unstable_IdlePriority=5,e.unstable_ImmediatePriority=1,e.unstable_LowPriority=4,e.unstable_NormalPriority=3,e.unstable_Profiling=null,e.unstable_UserBlockingPriority=2,e.unstable_cancelCallback=function(S){S.callback=null},e.unstable_continueExecution=function(){v||y||(v=!0,pe(T))},e.unstable_forceFrameRate=function(S){0>S||125<S?console.error("forceFrameRate takes a positive int between 0 and 125, forcing frame rates higher than 125 fps is not supported"):P=0<S?Math.floor(1e3/S):5},e.unstable_getCurrentPriorityLevel=function(){return g},e.unstable_getFirstCallbackNode=function(){return n(c)},e.unstable_next=function(S){switch(g){case 1:case 2:case 3:var R=3;break;default:R=g}var M=g;g=R;try{return S()}finally{g=M}},e.unstable_pauseExecution=function(){},e.unstable_requestPaint=function(){},e.unstable_runWithPriority=function(S,R){switch(S){case 1:case 2:case 3:case 4:case 5:break;default:S=3}var M=g;g=S;try{return R()}finally{g=M}},e.unstable_scheduleCallback=function(S,R,M){var U=e.unstable_now();switch(typeof M=="object"&&M!==null?(M=M.delay,M=typeof M=="number"&&0<M?U+M:U):M=U,S){case 1:var D=-1;break;case 2:D=250;break;case 5:D=1073741823;break;case 4:D=1e4;break;default:D=5e3}return D=M+D,S={id:h++,callback:R,priorityLevel:S,startTime:M,expirationTime:D,sortIndex:-1},M>U?(S.sortIndex=M,t(d,S),n(c)===null&&S===n(d)&&(b?(p(_),_=-1):b=!0,I(j,M-U))):(S.sortIndex=D,t(c,S),v||y||(v=!0,pe(T))),S},e.unstable_shouldYield=B,e.unstable_wrapCallback=function(S){var R=g;return function(){var M=g;g=R;try{return S.apply(this,arguments)}finally{g=M}}}})(Tc);Cc.exports=Tc;var Sp=Cc.exports;/**
 * @license React
 * react-dom.production.min.js
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var Np=x,He=Sp;function L(e){for(var t="https://reactjs.org/docs/error-decoder.html?invariant="+e,n=1;n<arguments.length;n++)t+="&args[]="+encodeURIComponent(arguments[n]);return"Minified React error #"+e+"; visit "+t+" for the full message or use the non-minified dev environment for full errors and additional helpful warnings."}var Ec=new Set,vr={};function on(e,t){Ln(e,t),Ln(e+"Capture",t)}function Ln(e,t){for(vr[e]=t,e=0;e<t.length;e++)Ec.add(t[e])}var yt=!(typeof window>"u"||typeof window.document>"u"||typeof window.document.createElement>"u"),$i=Object.prototype.hasOwnProperty,Cp=/^[:A-Z_a-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD][:A-Z_a-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\-.0-9\u00B7\u0300-\u036F\u203F-\u2040]*$/,Xs={},Qs={};function Tp(e){return $i.call(Qs,e)?!0:$i.call(Xs,e)?!1:Cp.test(e)?Qs[e]=!0:(Xs[e]=!0,!1)}function Ep(e,t,n,o){if(n!==null&&n.type===0)return!1;switch(typeof t){case"function":case"symbol":return!0;case"boolean":return o?!1:n!==null?!n.acceptsBooleans:(e=e.toLowerCase().slice(0,5),e!=="data-"&&e!=="aria-");default:return!1}}function zp(e,t,n,o){if(t===null||typeof t>"u"||Ep(e,t,n,o))return!0;if(o)return!1;if(n!==null)switch(n.type){case 3:return!t;case 4:return t===!1;case 5:return isNaN(t);case 6:return isNaN(t)||1>t}return!1}function Le(e,t,n,o,i,a,s){this.acceptsBooleans=t===2||t===3||t===4,this.attributeName=o,this.attributeNamespace=i,this.mustUseProperty=n,this.propertyName=e,this.type=t,this.sanitizeURL=a,this.removeEmptyString=s}var je={};"children dangerouslySetInnerHTML defaultValue defaultChecked innerHTML suppressContentEditableWarning suppressHydrationWarning style".split(" ").forEach(function(e){je[e]=new Le(e,0,!1,e,null,!1,!1)});[["acceptCharset","accept-charset"],["className","class"],["htmlFor","for"],["httpEquiv","http-equiv"]].forEach(function(e){var t=e[0];je[t]=new Le(t,1,!1,e[1],null,!1,!1)});["contentEditable","draggable","spellCheck","value"].forEach(function(e){je[e]=new Le(e,2,!1,e.toLowerCase(),null,!1,!1)});["autoReverse","externalResourcesRequired","focusable","preserveAlpha"].forEach(function(e){je[e]=new Le(e,2,!1,e,null,!1,!1)});"allowFullScreen async autoFocus autoPlay controls default defer disabled disablePictureInPicture disableRemotePlayback formNoValidate hidden loop noModule noValidate open playsInline readOnly required reversed scoped seamless itemScope".split(" ").forEach(function(e){je[e]=new Le(e,3,!1,e.toLowerCase(),null,!1,!1)});["checked","multiple","muted","selected"].forEach(function(e){je[e]=new Le(e,3,!0,e,null,!1,!1)});["capture","download"].forEach(function(e){je[e]=new Le(e,4,!1,e,null,!1,!1)});["cols","rows","size","span"].forEach(function(e){je[e]=new Le(e,6,!1,e,null,!1,!1)});["rowSpan","start"].forEach(function(e){je[e]=new Le(e,5,!1,e.toLowerCase(),null,!1,!1)});var Fa=/[\-:]([a-z])/g;function Ua(e){return e[1].toUpperCase()}"accent-height alignment-baseline arabic-form baseline-shift cap-height clip-path clip-rule color-interpolation color-interpolation-filters color-profile color-rendering dominant-baseline enable-background fill-opacity fill-rule flood-color flood-opacity font-family font-size font-size-adjust font-stretch font-style font-variant font-weight glyph-name glyph-orientation-horizontal glyph-orientation-vertical horiz-adv-x horiz-origin-x image-rendering letter-spacing lighting-color marker-end marker-mid marker-start overline-position overline-thickness paint-order panose-1 pointer-events rendering-intent shape-rendering stop-color stop-opacity strikethrough-position strikethrough-thickness stroke-dasharray stroke-dashoffset stroke-linecap stroke-linejoin stroke-miterlimit stroke-opacity stroke-width text-anchor text-decoration text-rendering underline-position underline-thickness unicode-bidi unicode-range units-per-em v-alphabetic v-hanging v-ideographic v-mathematical vector-effect vert-adv-y vert-origin-x vert-origin-y word-spacing writing-mode xmlns:xlink x-height".split(" ").forEach(function(e){var t=e.replace(Fa,Ua);je[t]=new Le(t,1,!1,e,null,!1,!1)});"xlink:actuate xlink:arcrole xlink:role xlink:show xlink:title xlink:type".split(" ").forEach(function(e){var t=e.replace(Fa,Ua);je[t]=new Le(t,1,!1,e,"http://www.w3.org/1999/xlink",!1,!1)});["xml:base","xml:lang","xml:space"].forEach(function(e){var t=e.replace(Fa,Ua);je[t]=new Le(t,1,!1,e,"http://www.w3.org/XML/1998/namespace",!1,!1)});["tabIndex","crossOrigin"].forEach(function(e){je[e]=new Le(e,1,!1,e.toLowerCase(),null,!1,!1)});je.xlinkHref=new Le("xlinkHref",1,!1,"xlink:href","http://www.w3.org/1999/xlink",!0,!1);["src","href","action","formAction"].forEach(function(e){je[e]=new Le(e,1,!1,e.toLowerCase(),null,!0,!0)});function Wa(e,t,n,o){var i=je.hasOwnProperty(t)?je[t]:null;(i!==null?i.type!==0:o||!(2<t.length)||t[0]!=="o"&&t[0]!=="O"||t[1]!=="n"&&t[1]!=="N")&&(zp(t,n,i,o)&&(n=null),o||i===null?Tp(t)&&(n===null?e.removeAttribute(t):e.setAttribute(t,""+n)):i.mustUseProperty?e[i.propertyName]=n===null?i.type===3?!1:"":n:(t=i.attributeName,o=i.attributeNamespace,n===null?e.removeAttribute(t):(i=i.type,n=i===3||i===4&&n===!0?"":""+n,o?e.setAttributeNS(o,t,n):e.setAttribute(t,n))))}var wt=Np.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED,Wr=Symbol.for("react.element"),mn=Symbol.for("react.portal"),gn=Symbol.for("react.fragment"),$a=Symbol.for("react.strict_mode"),Hi=Symbol.for("react.profiler"),zc=Symbol.for("react.provider"),_c=Symbol.for("react.context"),Ha=Symbol.for("react.forward_ref"),Ki=Symbol.for("react.suspense"),Vi=Symbol.for("react.suspense_list"),Ka=Symbol.for("react.memo"),Ct=Symbol.for("react.lazy"),Rc=Symbol.for("react.offscreen"),Ys=Symbol.iterator;function Xn(e){return e===null||typeof e!="object"?null:(e=Ys&&e[Ys]||e["@@iterator"],typeof e=="function"?e:null)}var se=Object.assign,mi;function or(e){if(mi===void 0)try{throw Error()}catch(n){var t=n.stack.trim().match(/\n( *(at )?)/);mi=t&&t[1]||""}return`
`+mi+e}var gi=!1;function hi(e,t){if(!e||gi)return"";gi=!0;var n=Error.prepareStackTrace;Error.prepareStackTrace=void 0;try{if(t)if(t=function(){throw Error()},Object.defineProperty(t.prototype,"props",{set:function(){throw Error()}}),typeof Reflect=="object"&&Reflect.construct){try{Reflect.construct(t,[])}catch(d){var o=d}Reflect.construct(e,[],t)}else{try{t.call()}catch(d){o=d}e.call(t.prototype)}else{try{throw Error()}catch(d){o=d}e()}}catch(d){if(d&&o&&typeof d.stack=="string"){for(var i=d.stack.split(`
`),a=o.stack.split(`
`),s=i.length-1,l=a.length-1;1<=s&&0<=l&&i[s]!==a[l];)l--;for(;1<=s&&0<=l;s--,l--)if(i[s]!==a[l]){if(s!==1||l!==1)do if(s--,l--,0>l||i[s]!==a[l]){var c=`
`+i[s].replace(" at new "," at ");return e.displayName&&c.includes("<anonymous>")&&(c=c.replace("<anonymous>",e.displayName)),c}while(1<=s&&0<=l);break}}}finally{gi=!1,Error.prepareStackTrace=n}return(e=e?e.displayName||e.name:"")?or(e):""}function _p(e){switch(e.tag){case 5:return or(e.type);case 16:return or("Lazy");case 13:return or("Suspense");case 19:return or("SuspenseList");case 0:case 2:case 15:return e=hi(e.type,!1),e;case 11:return e=hi(e.type.render,!1),e;case 1:return e=hi(e.type,!0),e;default:return""}}function Gi(e){if(e==null)return null;if(typeof e=="function")return e.displayName||e.name||null;if(typeof e=="string")return e;switch(e){case gn:return"Fragment";case mn:return"Portal";case Hi:return"Profiler";case $a:return"StrictMode";case Ki:return"Suspense";case Vi:return"SuspenseList"}if(typeof e=="object")switch(e.$$typeof){case _c:return(e.displayName||"Context")+".Consumer";case zc:return(e._context.displayName||"Context")+".Provider";case Ha:var t=e.render;return e=e.displayName,e||(e=t.displayName||t.name||"",e=e!==""?"ForwardRef("+e+")":"ForwardRef"),e;case Ka:return t=e.displayName||null,t!==null?t:Gi(e.type)||"Memo";case Ct:t=e._payload,e=e._init;try{return Gi(e(t))}catch{}}return null}function Rp(e){var t=e.type;switch(e.tag){case 24:return"Cache";case 9:return(t.displayName||"Context")+".Consumer";case 10:return(t._context.displayName||"Context")+".Provider";case 18:return"DehydratedFragment";case 11:return e=t.render,e=e.displayName||e.name||"",t.displayName||(e!==""?"ForwardRef("+e+")":"ForwardRef");case 7:return"Fragment";case 5:return t;case 4:return"Portal";case 3:return"Root";case 6:return"Text";case 16:return Gi(t);case 8:return t===$a?"StrictMode":"Mode";case 22:return"Offscreen";case 12:return"Profiler";case 21:return"Scope";case 13:return"Suspense";case 19:return"SuspenseList";case 25:return"TracingMarker";case 1:case 0:case 17:case 2:case 14:case 15:if(typeof t=="function")return t.displayName||t.name||null;if(typeof t=="string")return t}return null}function Ft(e){switch(typeof e){case"boolean":case"number":case"string":case"undefined":return e;case"object":return e;default:return""}}function Lc(e){var t=e.type;return(e=e.nodeName)&&e.toLowerCase()==="input"&&(t==="checkbox"||t==="radio")}function Lp(e){var t=Lc(e)?"checked":"value",n=Object.getOwnPropertyDescriptor(e.constructor.prototype,t),o=""+e[t];if(!e.hasOwnProperty(t)&&typeof n<"u"&&typeof n.get=="function"&&typeof n.set=="function"){var i=n.get,a=n.set;return Object.defineProperty(e,t,{configurable:!0,get:function(){return i.call(this)},set:function(s){o=""+s,a.call(this,s)}}),Object.defineProperty(e,t,{enumerable:n.enumerable}),{getValue:function(){return o},setValue:function(s){o=""+s},stopTracking:function(){e._valueTracker=null,delete e[t]}}}}function $r(e){e._valueTracker||(e._valueTracker=Lp(e))}function Pc(e){if(!e)return!1;var t=e._valueTracker;if(!t)return!0;var n=t.getValue(),o="";return e&&(o=Lc(e)?e.checked?"true":"false":e.value),e=o,e!==n?(t.setValue(e),!0):!1}function ko(e){if(e=e||(typeof document<"u"?document:void 0),typeof e>"u")return null;try{return e.activeElement||e.body}catch{return e.body}}function Xi(e,t){var n=t.checked;return se({},t,{defaultChecked:void 0,defaultValue:void 0,value:void 0,checked:n??e._wrapperState.initialChecked})}function Js(e,t){var n=t.defaultValue==null?"":t.defaultValue,o=t.checked!=null?t.checked:t.defaultChecked;n=Ft(t.value!=null?t.value:n),e._wrapperState={initialChecked:o,initialValue:n,controlled:t.type==="checkbox"||t.type==="radio"?t.checked!=null:t.value!=null}}function Dc(e,t){t=t.checked,t!=null&&Wa(e,"checked",t,!1)}function Qi(e,t){Dc(e,t);var n=Ft(t.value),o=t.type;if(n!=null)o==="number"?(n===0&&e.value===""||e.value!=n)&&(e.value=""+n):e.value!==""+n&&(e.value=""+n);else if(o==="submit"||o==="reset"){e.removeAttribute("value");return}t.hasOwnProperty("value")?Yi(e,t.type,n):t.hasOwnProperty("defaultValue")&&Yi(e,t.type,Ft(t.defaultValue)),t.checked==null&&t.defaultChecked!=null&&(e.defaultChecked=!!t.defaultChecked)}function Zs(e,t,n){if(t.hasOwnProperty("value")||t.hasOwnProperty("defaultValue")){var o=t.type;if(!(o!=="submit"&&o!=="reset"||t.value!==void 0&&t.value!==null))return;t=""+e._wrapperState.initialValue,n||t===e.value||(e.value=t),e.defaultValue=t}n=e.name,n!==""&&(e.name=""),e.defaultChecked=!!e._wrapperState.initialChecked,n!==""&&(e.name=n)}function Yi(e,t,n){(t!=="number"||ko(e.ownerDocument)!==e)&&(n==null?e.defaultValue=""+e._wrapperState.initialValue:e.defaultValue!==""+n&&(e.defaultValue=""+n))}var ir=Array.isArray;function Cn(e,t,n,o){if(e=e.options,t){t={};for(var i=0;i<n.length;i++)t["$"+n[i]]=!0;for(n=0;n<e.length;n++)i=t.hasOwnProperty("$"+e[n].value),e[n].selected!==i&&(e[n].selected=i),i&&o&&(e[n].defaultSelected=!0)}else{for(n=""+Ft(n),t=null,i=0;i<e.length;i++){if(e[i].value===n){e[i].selected=!0,o&&(e[i].defaultSelected=!0);return}t!==null||e[i].disabled||(t=e[i])}t!==null&&(t.selected=!0)}}function Ji(e,t){if(t.dangerouslySetInnerHTML!=null)throw Error(L(91));return se({},t,{value:void 0,defaultValue:void 0,children:""+e._wrapperState.initialValue})}function qs(e,t){var n=t.value;if(n==null){if(n=t.children,t=t.defaultValue,n!=null){if(t!=null)throw Error(L(92));if(ir(n)){if(1<n.length)throw Error(L(93));n=n[0]}t=n}t==null&&(t=""),n=t}e._wrapperState={initialValue:Ft(n)}}function Ic(e,t){var n=Ft(t.value),o=Ft(t.defaultValue);n!=null&&(n=""+n,n!==e.value&&(e.value=n),t.defaultValue==null&&e.defaultValue!==n&&(e.defaultValue=n)),o!=null&&(e.defaultValue=""+o)}function el(e){var t=e.textContent;t===e._wrapperState.initialValue&&t!==""&&t!==null&&(e.value=t)}function Mc(e){switch(e){case"svg":return"http://www.w3.org/2000/svg";case"math":return"http://www.w3.org/1998/Math/MathML";default:return"http://www.w3.org/1999/xhtml"}}function Zi(e,t){return e==null||e==="http://www.w3.org/1999/xhtml"?Mc(t):e==="http://www.w3.org/2000/svg"&&t==="foreignObject"?"http://www.w3.org/1999/xhtml":e}var Hr,Ac=function(e){return typeof MSApp<"u"&&MSApp.execUnsafeLocalFunction?function(t,n,o,i){MSApp.execUnsafeLocalFunction(function(){return e(t,n,o,i)})}:e}(function(e,t){if(e.namespaceURI!=="http://www.w3.org/2000/svg"||"innerHTML"in e)e.innerHTML=t;else{for(Hr=Hr||document.createElement("div"),Hr.innerHTML="<svg>"+t.valueOf().toString()+"</svg>",t=Hr.firstChild;e.firstChild;)e.removeChild(e.firstChild);for(;t.firstChild;)e.appendChild(t.firstChild)}});function yr(e,t){if(t){var n=e.firstChild;if(n&&n===e.lastChild&&n.nodeType===3){n.nodeValue=t;return}}e.textContent=t}var cr={animationIterationCount:!0,aspectRatio:!0,borderImageOutset:!0,borderImageSlice:!0,borderImageWidth:!0,boxFlex:!0,boxFlexGroup:!0,boxOrdinalGroup:!0,columnCount:!0,columns:!0,flex:!0,flexGrow:!0,flexPositive:!0,flexShrink:!0,flexNegative:!0,flexOrder:!0,gridArea:!0,gridRow:!0,gridRowEnd:!0,gridRowSpan:!0,gridRowStart:!0,gridColumn:!0,gridColumnEnd:!0,gridColumnSpan:!0,gridColumnStart:!0,fontWeight:!0,lineClamp:!0,lineHeight:!0,opacity:!0,order:!0,orphans:!0,tabSize:!0,widows:!0,zIndex:!0,zoom:!0,fillOpacity:!0,floodOpacity:!0,stopOpacity:!0,strokeDasharray:!0,strokeDashoffset:!0,strokeMiterlimit:!0,strokeOpacity:!0,strokeWidth:!0},Pp=["Webkit","ms","Moz","O"];Object.keys(cr).forEach(function(e){Pp.forEach(function(t){t=t+e.charAt(0).toUpperCase()+e.substring(1),cr[t]=cr[e]})});function Oc(e,t,n){return t==null||typeof t=="boolean"||t===""?"":n||typeof t!="number"||t===0||cr.hasOwnProperty(e)&&cr[e]?(""+t).trim():t+"px"}function Bc(e,t){e=e.style;for(var n in t)if(t.hasOwnProperty(n)){var o=n.indexOf("--")===0,i=Oc(n,t[n],o);n==="float"&&(n="cssFloat"),o?e.setProperty(n,i):e[n]=i}}var Dp=se({menuitem:!0},{area:!0,base:!0,br:!0,col:!0,embed:!0,hr:!0,img:!0,input:!0,keygen:!0,link:!0,meta:!0,param:!0,source:!0,track:!0,wbr:!0});function qi(e,t){if(t){if(Dp[e]&&(t.children!=null||t.dangerouslySetInnerHTML!=null))throw Error(L(137,e));if(t.dangerouslySetInnerHTML!=null){if(t.children!=null)throw Error(L(60));if(typeof t.dangerouslySetInnerHTML!="object"||!("__html"in t.dangerouslySetInnerHTML))throw Error(L(61))}if(t.style!=null&&typeof t.style!="object")throw Error(L(62))}}function ea(e,t){if(e.indexOf("-")===-1)return typeof t.is=="string";switch(e){case"annotation-xml":case"color-profile":case"font-face":case"font-face-src":case"font-face-uri":case"font-face-format":case"font-face-name":case"missing-glyph":return!1;default:return!0}}var ta=null;function Va(e){return e=e.target||e.srcElement||window,e.correspondingUseElement&&(e=e.correspondingUseElement),e.nodeType===3?e.parentNode:e}var na=null,Tn=null,En=null;function tl(e){if(e=Or(e)){if(typeof na!="function")throw Error(L(280));var t=e.stateNode;t&&(t=Jo(t),na(e.stateNode,e.type,t))}}function Fc(e){Tn?En?En.push(e):En=[e]:Tn=e}function Uc(){if(Tn){var e=Tn,t=En;if(En=Tn=null,tl(e),t)for(e=0;e<t.length;e++)tl(t[e])}}function Wc(e,t){return e(t)}function $c(){}var xi=!1;function Hc(e,t,n){if(xi)return e(t,n);xi=!0;try{return Wc(e,t,n)}finally{xi=!1,(Tn!==null||En!==null)&&($c(),Uc())}}function br(e,t){var n=e.stateNode;if(n===null)return null;var o=Jo(n);if(o===null)return null;n=o[t];e:switch(t){case"onClick":case"onClickCapture":case"onDoubleClick":case"onDoubleClickCapture":case"onMouseDown":case"onMouseDownCapture":case"onMouseMove":case"onMouseMoveCapture":case"onMouseUp":case"onMouseUpCapture":case"onMouseEnter":(o=!o.disabled)||(e=e.type,o=!(e==="button"||e==="input"||e==="select"||e==="textarea")),e=!o;break e;default:e=!1}if(e)return null;if(n&&typeof n!="function")throw Error(L(231,t,typeof n));return n}var ra=!1;if(yt)try{var Qn={};Object.defineProperty(Qn,"passive",{get:function(){ra=!0}}),window.addEventListener("test",Qn,Qn),window.removeEventListener("test",Qn,Qn)}catch{ra=!1}function Ip(e,t,n,o,i,a,s,l,c){var d=Array.prototype.slice.call(arguments,3);try{t.apply(n,d)}catch(h){this.onError(h)}}var dr=!1,jo=null,wo=!1,oa=null,Mp={onError:function(e){dr=!0,jo=e}};function Ap(e,t,n,o,i,a,s,l,c){dr=!1,jo=null,Ip.apply(Mp,arguments)}function Op(e,t,n,o,i,a,s,l,c){if(Ap.apply(this,arguments),dr){if(dr){var d=jo;dr=!1,jo=null}else throw Error(L(198));wo||(wo=!0,oa=d)}}function an(e){var t=e,n=e;if(e.alternate)for(;t.return;)t=t.return;else{e=t;do t=e,t.flags&4098&&(n=t.return),e=t.return;while(e)}return t.tag===3?n:null}function Kc(e){if(e.tag===13){var t=e.memoizedState;if(t===null&&(e=e.alternate,e!==null&&(t=e.memoizedState)),t!==null)return t.dehydrated}return null}function nl(e){if(an(e)!==e)throw Error(L(188))}function Bp(e){var t=e.alternate;if(!t){if(t=an(e),t===null)throw Error(L(188));return t!==e?null:e}for(var n=e,o=t;;){var i=n.return;if(i===null)break;var a=i.alternate;if(a===null){if(o=i.return,o!==null){n=o;continue}break}if(i.child===a.child){for(a=i.child;a;){if(a===n)return nl(i),e;if(a===o)return nl(i),t;a=a.sibling}throw Error(L(188))}if(n.return!==o.return)n=i,o=a;else{for(var s=!1,l=i.child;l;){if(l===n){s=!0,n=i,o=a;break}if(l===o){s=!0,o=i,n=a;break}l=l.sibling}if(!s){for(l=a.child;l;){if(l===n){s=!0,n=a,o=i;break}if(l===o){s=!0,o=a,n=i;break}l=l.sibling}if(!s)throw Error(L(189))}}if(n.alternate!==o)throw Error(L(190))}if(n.tag!==3)throw Error(L(188));return n.stateNode.current===n?e:t}function Vc(e){return e=Bp(e),e!==null?Gc(e):null}function Gc(e){if(e.tag===5||e.tag===6)return e;for(e=e.child;e!==null;){var t=Gc(e);if(t!==null)return t;e=e.sibling}return null}var Xc=He.unstable_scheduleCallback,rl=He.unstable_cancelCallback,Fp=He.unstable_shouldYield,Up=He.unstable_requestPaint,de=He.unstable_now,Wp=He.unstable_getCurrentPriorityLevel,Ga=He.unstable_ImmediatePriority,Qc=He.unstable_UserBlockingPriority,So=He.unstable_NormalPriority,$p=He.unstable_LowPriority,Yc=He.unstable_IdlePriority,Go=null,ut=null;function Hp(e){if(ut&&typeof ut.onCommitFiberRoot=="function")try{ut.onCommitFiberRoot(Go,e,void 0,(e.current.flags&128)===128)}catch{}}var it=Math.clz32?Math.clz32:Gp,Kp=Math.log,Vp=Math.LN2;function Gp(e){return e>>>=0,e===0?32:31-(Kp(e)/Vp|0)|0}var Kr=64,Vr=4194304;function ar(e){switch(e&-e){case 1:return 1;case 2:return 2;case 4:return 4;case 8:return 8;case 16:return 16;case 32:return 32;case 64:case 128:case 256:case 512:case 1024:case 2048:case 4096:case 8192:case 16384:case 32768:case 65536:case 131072:case 262144:case 524288:case 1048576:case 2097152:return e&4194240;case 4194304:case 8388608:case 16777216:case 33554432:case 67108864:return e&130023424;case 134217728:return 134217728;case 268435456:return 268435456;case 536870912:return 536870912;case 1073741824:return 1073741824;default:return e}}function No(e,t){var n=e.pendingLanes;if(n===0)return 0;var o=0,i=e.suspendedLanes,a=e.pingedLanes,s=n&268435455;if(s!==0){var l=s&~i;l!==0?o=ar(l):(a&=s,a!==0&&(o=ar(a)))}else s=n&~i,s!==0?o=ar(s):a!==0&&(o=ar(a));if(o===0)return 0;if(t!==0&&t!==o&&!(t&i)&&(i=o&-o,a=t&-t,i>=a||i===16&&(a&4194240)!==0))return t;if(o&4&&(o|=n&16),t=e.entangledLanes,t!==0)for(e=e.entanglements,t&=o;0<t;)n=31-it(t),i=1<<n,o|=e[n],t&=~i;return o}function Xp(e,t){switch(e){case 1:case 2:case 4:return t+250;case 8:case 16:case 32:case 64:case 128:case 256:case 512:case 1024:case 2048:case 4096:case 8192:case 16384:case 32768:case 65536:case 131072:case 262144:case 524288:case 1048576:case 2097152:return t+5e3;case 4194304:case 8388608:case 16777216:case 33554432:case 67108864:return-1;case 134217728:case 268435456:case 536870912:case 1073741824:return-1;default:return-1}}function Qp(e,t){for(var n=e.suspendedLanes,o=e.pingedLanes,i=e.expirationTimes,a=e.pendingLanes;0<a;){var s=31-it(a),l=1<<s,c=i[s];c===-1?(!(l&n)||l&o)&&(i[s]=Xp(l,t)):c<=t&&(e.expiredLanes|=l),a&=~l}}function ia(e){return e=e.pendingLanes&-1073741825,e!==0?e:e&1073741824?1073741824:0}function Jc(){var e=Kr;return Kr<<=1,!(Kr&4194240)&&(Kr=64),e}function vi(e){for(var t=[],n=0;31>n;n++)t.push(e);return t}function Mr(e,t,n){e.pendingLanes|=t,t!==536870912&&(e.suspendedLanes=0,e.pingedLanes=0),e=e.eventTimes,t=31-it(t),e[t]=n}function Yp(e,t){var n=e.pendingLanes&~t;e.pendingLanes=t,e.suspendedLanes=0,e.pingedLanes=0,e.expiredLanes&=t,e.mutableReadLanes&=t,e.entangledLanes&=t,t=e.entanglements;var o=e.eventTimes;for(e=e.expirationTimes;0<n;){var i=31-it(n),a=1<<i;t[i]=0,o[i]=-1,e[i]=-1,n&=~a}}function Xa(e,t){var n=e.entangledLanes|=t;for(e=e.entanglements;n;){var o=31-it(n),i=1<<o;i&t|e[o]&t&&(e[o]|=t),n&=~i}}var Z=0;function Zc(e){return e&=-e,1<e?4<e?e&268435455?16:536870912:4:1}var qc,Qa,ed,td,nd,aa=!1,Gr=[],Lt=null,Pt=null,Dt=null,kr=new Map,jr=new Map,Et=[],Jp="mousedown mouseup touchcancel touchend touchstart auxclick dblclick pointercancel pointerdown pointerup dragend dragstart drop compositionend compositionstart keydown keypress keyup input textInput copy cut paste click change contextmenu reset submit".split(" ");function ol(e,t){switch(e){case"focusin":case"focusout":Lt=null;break;case"dragenter":case"dragleave":Pt=null;break;case"mouseover":case"mouseout":Dt=null;break;case"pointerover":case"pointerout":kr.delete(t.pointerId);break;case"gotpointercapture":case"lostpointercapture":jr.delete(t.pointerId)}}function Yn(e,t,n,o,i,a){return e===null||e.nativeEvent!==a?(e={blockedOn:t,domEventName:n,eventSystemFlags:o,nativeEvent:a,targetContainers:[i]},t!==null&&(t=Or(t),t!==null&&Qa(t)),e):(e.eventSystemFlags|=o,t=e.targetContainers,i!==null&&t.indexOf(i)===-1&&t.push(i),e)}function Zp(e,t,n,o,i){switch(t){case"focusin":return Lt=Yn(Lt,e,t,n,o,i),!0;case"dragenter":return Pt=Yn(Pt,e,t,n,o,i),!0;case"mouseover":return Dt=Yn(Dt,e,t,n,o,i),!0;case"pointerover":var a=i.pointerId;return kr.set(a,Yn(kr.get(a)||null,e,t,n,o,i)),!0;case"gotpointercapture":return a=i.pointerId,jr.set(a,Yn(jr.get(a)||null,e,t,n,o,i)),!0}return!1}function rd(e){var t=Xt(e.target);if(t!==null){var n=an(t);if(n!==null){if(t=n.tag,t===13){if(t=Kc(n),t!==null){e.blockedOn=t,nd(e.priority,function(){ed(n)});return}}else if(t===3&&n.stateNode.current.memoizedState.isDehydrated){e.blockedOn=n.tag===3?n.stateNode.containerInfo:null;return}}}e.blockedOn=null}function uo(e){if(e.blockedOn!==null)return!1;for(var t=e.targetContainers;0<t.length;){var n=sa(e.domEventName,e.eventSystemFlags,t[0],e.nativeEvent);if(n===null){n=e.nativeEvent;var o=new n.constructor(n.type,n);ta=o,n.target.dispatchEvent(o),ta=null}else return t=Or(n),t!==null&&Qa(t),e.blockedOn=n,!1;t.shift()}return!0}function il(e,t,n){uo(e)&&n.delete(t)}function qp(){aa=!1,Lt!==null&&uo(Lt)&&(Lt=null),Pt!==null&&uo(Pt)&&(Pt=null),Dt!==null&&uo(Dt)&&(Dt=null),kr.forEach(il),jr.forEach(il)}function Jn(e,t){e.blockedOn===t&&(e.blockedOn=null,aa||(aa=!0,He.unstable_scheduleCallback(He.unstable_NormalPriority,qp)))}function wr(e){function t(i){return Jn(i,e)}if(0<Gr.length){Jn(Gr[0],e);for(var n=1;n<Gr.length;n++){var o=Gr[n];o.blockedOn===e&&(o.blockedOn=null)}}for(Lt!==null&&Jn(Lt,e),Pt!==null&&Jn(Pt,e),Dt!==null&&Jn(Dt,e),kr.forEach(t),jr.forEach(t),n=0;n<Et.length;n++)o=Et[n],o.blockedOn===e&&(o.blockedOn=null);for(;0<Et.length&&(n=Et[0],n.blockedOn===null);)rd(n),n.blockedOn===null&&Et.shift()}var zn=wt.ReactCurrentBatchConfig,Co=!0;function ef(e,t,n,o){var i=Z,a=zn.transition;zn.transition=null;try{Z=1,Ya(e,t,n,o)}finally{Z=i,zn.transition=a}}function tf(e,t,n,o){var i=Z,a=zn.transition;zn.transition=null;try{Z=4,Ya(e,t,n,o)}finally{Z=i,zn.transition=a}}function Ya(e,t,n,o){if(Co){var i=sa(e,t,n,o);if(i===null)Ei(e,t,o,To,n),ol(e,o);else if(Zp(i,e,t,n,o))o.stopPropagation();else if(ol(e,o),t&4&&-1<Jp.indexOf(e)){for(;i!==null;){var a=Or(i);if(a!==null&&qc(a),a=sa(e,t,n,o),a===null&&Ei(e,t,o,To,n),a===i)break;i=a}i!==null&&o.stopPropagation()}else Ei(e,t,o,null,n)}}var To=null;function sa(e,t,n,o){if(To=null,e=Va(o),e=Xt(e),e!==null)if(t=an(e),t===null)e=null;else if(n=t.tag,n===13){if(e=Kc(t),e!==null)return e;e=null}else if(n===3){if(t.stateNode.current.memoizedState.isDehydrated)return t.tag===3?t.stateNode.containerInfo:null;e=null}else t!==e&&(e=null);return To=e,null}function od(e){switch(e){case"cancel":case"click":case"close":case"contextmenu":case"copy":case"cut":case"auxclick":case"dblclick":case"dragend":case"dragstart":case"drop":case"focusin":case"focusout":case"input":case"invalid":case"keydown":case"keypress":case"keyup":case"mousedown":case"mouseup":case"paste":case"pause":case"play":case"pointercancel":case"pointerdown":case"pointerup":case"ratechange":case"reset":case"resize":case"seeked":case"submit":case"touchcancel":case"touchend":case"touchstart":case"volumechange":case"change":case"selectionchange":case"textInput":case"compositionstart":case"compositionend":case"compositionupdate":case"beforeblur":case"afterblur":case"beforeinput":case"blur":case"fullscreenchange":case"focus":case"hashchange":case"popstate":case"select":case"selectstart":return 1;case"drag":case"dragenter":case"dragexit":case"dragleave":case"dragover":case"mousemove":case"mouseout":case"mouseover":case"pointermove":case"pointerout":case"pointerover":case"scroll":case"toggle":case"touchmove":case"wheel":case"mouseenter":case"mouseleave":case"pointerenter":case"pointerleave":return 4;case"message":switch(Wp()){case Ga:return 1;case Qc:return 4;case So:case $p:return 16;case Yc:return 536870912;default:return 16}default:return 16}}var _t=null,Ja=null,po=null;function id(){if(po)return po;var e,t=Ja,n=t.length,o,i="value"in _t?_t.value:_t.textContent,a=i.length;for(e=0;e<n&&t[e]===i[e];e++);var s=n-e;for(o=1;o<=s&&t[n-o]===i[a-o];o++);return po=i.slice(e,1<o?1-o:void 0)}function fo(e){var t=e.keyCode;return"charCode"in e?(e=e.charCode,e===0&&t===13&&(e=13)):e=t,e===10&&(e=13),32<=e||e===13?e:0}function Xr(){return!0}function al(){return!1}function Ve(e){function t(n,o,i,a,s){this._reactName=n,this._targetInst=i,this.type=o,this.nativeEvent=a,this.target=s,this.currentTarget=null;for(var l in e)e.hasOwnProperty(l)&&(n=e[l],this[l]=n?n(a):a[l]);return this.isDefaultPrevented=(a.defaultPrevented!=null?a.defaultPrevented:a.returnValue===!1)?Xr:al,this.isPropagationStopped=al,this}return se(t.prototype,{preventDefault:function(){this.defaultPrevented=!0;var n=this.nativeEvent;n&&(n.preventDefault?n.preventDefault():typeof n.returnValue!="unknown"&&(n.returnValue=!1),this.isDefaultPrevented=Xr)},stopPropagation:function(){var n=this.nativeEvent;n&&(n.stopPropagation?n.stopPropagation():typeof n.cancelBubble!="unknown"&&(n.cancelBubble=!0),this.isPropagationStopped=Xr)},persist:function(){},isPersistent:Xr}),t}var Fn={eventPhase:0,bubbles:0,cancelable:0,timeStamp:function(e){return e.timeStamp||Date.now()},defaultPrevented:0,isTrusted:0},Za=Ve(Fn),Ar=se({},Fn,{view:0,detail:0}),nf=Ve(Ar),yi,bi,Zn,Xo=se({},Ar,{screenX:0,screenY:0,clientX:0,clientY:0,pageX:0,pageY:0,ctrlKey:0,shiftKey:0,altKey:0,metaKey:0,getModifierState:qa,button:0,buttons:0,relatedTarget:function(e){return e.relatedTarget===void 0?e.fromElement===e.srcElement?e.toElement:e.fromElement:e.relatedTarget},movementX:function(e){return"movementX"in e?e.movementX:(e!==Zn&&(Zn&&e.type==="mousemove"?(yi=e.screenX-Zn.screenX,bi=e.screenY-Zn.screenY):bi=yi=0,Zn=e),yi)},movementY:function(e){return"movementY"in e?e.movementY:bi}}),sl=Ve(Xo),rf=se({},Xo,{dataTransfer:0}),of=Ve(rf),af=se({},Ar,{relatedTarget:0}),ki=Ve(af),sf=se({},Fn,{animationName:0,elapsedTime:0,pseudoElement:0}),lf=Ve(sf),cf=se({},Fn,{clipboardData:function(e){return"clipboardData"in e?e.clipboardData:window.clipboardData}}),df=Ve(cf),uf=se({},Fn,{data:0}),ll=Ve(uf),pf={Esc:"Escape",Spacebar:" ",Left:"ArrowLeft",Up:"ArrowUp",Right:"ArrowRight",Down:"ArrowDown",Del:"Delete",Win:"OS",Menu:"ContextMenu",Apps:"ContextMenu",Scroll:"ScrollLock",MozPrintableKey:"Unidentified"},ff={8:"Backspace",9:"Tab",12:"Clear",13:"Enter",16:"Shift",17:"Control",18:"Alt",19:"Pause",20:"CapsLock",27:"Escape",32:" ",33:"PageUp",34:"PageDown",35:"End",36:"Home",37:"ArrowLeft",38:"ArrowUp",39:"ArrowRight",40:"ArrowDown",45:"Insert",46:"Delete",112:"F1",113:"F2",114:"F3",115:"F4",116:"F5",117:"F6",118:"F7",119:"F8",120:"F9",121:"F10",122:"F11",123:"F12",144:"NumLock",145:"ScrollLock",224:"Meta"},mf={Alt:"altKey",Control:"ctrlKey",Meta:"metaKey",Shift:"shiftKey"};function gf(e){var t=this.nativeEvent;return t.getModifierState?t.getModifierState(e):(e=mf[e])?!!t[e]:!1}function qa(){return gf}var hf=se({},Ar,{key:function(e){if(e.key){var t=pf[e.key]||e.key;if(t!=="Unidentified")return t}return e.type==="keypress"?(e=fo(e),e===13?"Enter":String.fromCharCode(e)):e.type==="keydown"||e.type==="keyup"?ff[e.keyCode]||"Unidentified":""},code:0,location:0,ctrlKey:0,shiftKey:0,altKey:0,metaKey:0,repeat:0,locale:0,getModifierState:qa,charCode:function(e){return e.type==="keypress"?fo(e):0},keyCode:function(e){return e.type==="keydown"||e.type==="keyup"?e.keyCode:0},which:function(e){return e.type==="keypress"?fo(e):e.type==="keydown"||e.type==="keyup"?e.keyCode:0}}),xf=Ve(hf),vf=se({},Xo,{pointerId:0,width:0,height:0,pressure:0,tangentialPressure:0,tiltX:0,tiltY:0,twist:0,pointerType:0,isPrimary:0}),cl=Ve(vf),yf=se({},Ar,{touches:0,targetTouches:0,changedTouches:0,altKey:0,metaKey:0,ctrlKey:0,shiftKey:0,getModifierState:qa}),bf=Ve(yf),kf=se({},Fn,{propertyName:0,elapsedTime:0,pseudoElement:0}),jf=Ve(kf),wf=se({},Xo,{deltaX:function(e){return"deltaX"in e?e.deltaX:"wheelDeltaX"in e?-e.wheelDeltaX:0},deltaY:function(e){return"deltaY"in e?e.deltaY:"wheelDeltaY"in e?-e.wheelDeltaY:"wheelDelta"in e?-e.wheelDelta:0},deltaZ:0,deltaMode:0}),Sf=Ve(wf),Nf=[9,13,27,32],es=yt&&"CompositionEvent"in window,ur=null;yt&&"documentMode"in document&&(ur=document.documentMode);var Cf=yt&&"TextEvent"in window&&!ur,ad=yt&&(!es||ur&&8<ur&&11>=ur),dl=" ",ul=!1;function sd(e,t){switch(e){case"keyup":return Nf.indexOf(t.keyCode)!==-1;case"keydown":return t.keyCode!==229;case"keypress":case"mousedown":case"focusout":return!0;default:return!1}}function ld(e){return e=e.detail,typeof e=="object"&&"data"in e?e.data:null}var hn=!1;function Tf(e,t){switch(e){case"compositionend":return ld(t);case"keypress":return t.which!==32?null:(ul=!0,dl);case"textInput":return e=t.data,e===dl&&ul?null:e;default:return null}}function Ef(e,t){if(hn)return e==="compositionend"||!es&&sd(e,t)?(e=id(),po=Ja=_t=null,hn=!1,e):null;switch(e){case"paste":return null;case"keypress":if(!(t.ctrlKey||t.altKey||t.metaKey)||t.ctrlKey&&t.altKey){if(t.char&&1<t.char.length)return t.char;if(t.which)return String.fromCharCode(t.which)}return null;case"compositionend":return ad&&t.locale!=="ko"?null:t.data;default:return null}}var zf={color:!0,date:!0,datetime:!0,"datetime-local":!0,email:!0,month:!0,number:!0,password:!0,range:!0,search:!0,tel:!0,text:!0,time:!0,url:!0,week:!0};function pl(e){var t=e&&e.nodeName&&e.nodeName.toLowerCase();return t==="input"?!!zf[e.type]:t==="textarea"}function cd(e,t,n,o){Fc(o),t=Eo(t,"onChange"),0<t.length&&(n=new Za("onChange","change",null,n,o),e.push({event:n,listeners:t}))}var pr=null,Sr=null;function _f(e){bd(e,0)}function Qo(e){var t=yn(e);if(Pc(t))return e}function Rf(e,t){if(e==="change")return t}var dd=!1;if(yt){var ji;if(yt){var wi="oninput"in document;if(!wi){var fl=document.createElement("div");fl.setAttribute("oninput","return;"),wi=typeof fl.oninput=="function"}ji=wi}else ji=!1;dd=ji&&(!document.documentMode||9<document.documentMode)}function ml(){pr&&(pr.detachEvent("onpropertychange",ud),Sr=pr=null)}function ud(e){if(e.propertyName==="value"&&Qo(Sr)){var t=[];cd(t,Sr,e,Va(e)),Hc(_f,t)}}function Lf(e,t,n){e==="focusin"?(ml(),pr=t,Sr=n,pr.attachEvent("onpropertychange",ud)):e==="focusout"&&ml()}function Pf(e){if(e==="selectionchange"||e==="keyup"||e==="keydown")return Qo(Sr)}function Df(e,t){if(e==="click")return Qo(t)}function If(e,t){if(e==="input"||e==="change")return Qo(t)}function Mf(e,t){return e===t&&(e!==0||1/e===1/t)||e!==e&&t!==t}var st=typeof Object.is=="function"?Object.is:Mf;function Nr(e,t){if(st(e,t))return!0;if(typeof e!="object"||e===null||typeof t!="object"||t===null)return!1;var n=Object.keys(e),o=Object.keys(t);if(n.length!==o.length)return!1;for(o=0;o<n.length;o++){var i=n[o];if(!$i.call(t,i)||!st(e[i],t[i]))return!1}return!0}function gl(e){for(;e&&e.firstChild;)e=e.firstChild;return e}function hl(e,t){var n=gl(e);e=0;for(var o;n;){if(n.nodeType===3){if(o=e+n.textContent.length,e<=t&&o>=t)return{node:n,offset:t-e};e=o}e:{for(;n;){if(n.nextSibling){n=n.nextSibling;break e}n=n.parentNode}n=void 0}n=gl(n)}}function pd(e,t){return e&&t?e===t?!0:e&&e.nodeType===3?!1:t&&t.nodeType===3?pd(e,t.parentNode):"contains"in e?e.contains(t):e.compareDocumentPosition?!!(e.compareDocumentPosition(t)&16):!1:!1}function fd(){for(var e=window,t=ko();t instanceof e.HTMLIFrameElement;){try{var n=typeof t.contentWindow.location.href=="string"}catch{n=!1}if(n)e=t.contentWindow;else break;t=ko(e.document)}return t}function ts(e){var t=e&&e.nodeName&&e.nodeName.toLowerCase();return t&&(t==="input"&&(e.type==="text"||e.type==="search"||e.type==="tel"||e.type==="url"||e.type==="password")||t==="textarea"||e.contentEditable==="true")}function Af(e){var t=fd(),n=e.focusedElem,o=e.selectionRange;if(t!==n&&n&&n.ownerDocument&&pd(n.ownerDocument.documentElement,n)){if(o!==null&&ts(n)){if(t=o.start,e=o.end,e===void 0&&(e=t),"selectionStart"in n)n.selectionStart=t,n.selectionEnd=Math.min(e,n.value.length);else if(e=(t=n.ownerDocument||document)&&t.defaultView||window,e.getSelection){e=e.getSelection();var i=n.textContent.length,a=Math.min(o.start,i);o=o.end===void 0?a:Math.min(o.end,i),!e.extend&&a>o&&(i=o,o=a,a=i),i=hl(n,a);var s=hl(n,o);i&&s&&(e.rangeCount!==1||e.anchorNode!==i.node||e.anchorOffset!==i.offset||e.focusNode!==s.node||e.focusOffset!==s.offset)&&(t=t.createRange(),t.setStart(i.node,i.offset),e.removeAllRanges(),a>o?(e.addRange(t),e.extend(s.node,s.offset)):(t.setEnd(s.node,s.offset),e.addRange(t)))}}for(t=[],e=n;e=e.parentNode;)e.nodeType===1&&t.push({element:e,left:e.scrollLeft,top:e.scrollTop});for(typeof n.focus=="function"&&n.focus(),n=0;n<t.length;n++)e=t[n],e.element.scrollLeft=e.left,e.element.scrollTop=e.top}}var Of=yt&&"documentMode"in document&&11>=document.documentMode,xn=null,la=null,fr=null,ca=!1;function xl(e,t,n){var o=n.window===n?n.document:n.nodeType===9?n:n.ownerDocument;ca||xn==null||xn!==ko(o)||(o=xn,"selectionStart"in o&&ts(o)?o={start:o.selectionStart,end:o.selectionEnd}:(o=(o.ownerDocument&&o.ownerDocument.defaultView||window).getSelection(),o={anchorNode:o.anchorNode,anchorOffset:o.anchorOffset,focusNode:o.focusNode,focusOffset:o.focusOffset}),fr&&Nr(fr,o)||(fr=o,o=Eo(la,"onSelect"),0<o.length&&(t=new Za("onSelect","select",null,t,n),e.push({event:t,listeners:o}),t.target=xn)))}function Qr(e,t){var n={};return n[e.toLowerCase()]=t.toLowerCase(),n["Webkit"+e]="webkit"+t,n["Moz"+e]="moz"+t,n}var vn={animationend:Qr("Animation","AnimationEnd"),animationiteration:Qr("Animation","AnimationIteration"),animationstart:Qr("Animation","AnimationStart"),transitionend:Qr("Transition","TransitionEnd")},Si={},md={};yt&&(md=document.createElement("div").style,"AnimationEvent"in window||(delete vn.animationend.animation,delete vn.animationiteration.animation,delete vn.animationstart.animation),"TransitionEvent"in window||delete vn.transitionend.transition);function Yo(e){if(Si[e])return Si[e];if(!vn[e])return e;var t=vn[e],n;for(n in t)if(t.hasOwnProperty(n)&&n in md)return Si[e]=t[n];return e}var gd=Yo("animationend"),hd=Yo("animationiteration"),xd=Yo("animationstart"),vd=Yo("transitionend"),yd=new Map,vl="abort auxClick cancel canPlay canPlayThrough click close contextMenu copy cut drag dragEnd dragEnter dragExit dragLeave dragOver dragStart drop durationChange emptied encrypted ended error gotPointerCapture input invalid keyDown keyPress keyUp load loadedData loadedMetadata loadStart lostPointerCapture mouseDown mouseMove mouseOut mouseOver mouseUp paste pause play playing pointerCancel pointerDown pointerMove pointerOut pointerOver pointerUp progress rateChange reset resize seeked seeking stalled submit suspend timeUpdate touchCancel touchEnd touchStart volumeChange scroll toggle touchMove waiting wheel".split(" ");function Wt(e,t){yd.set(e,t),on(t,[e])}for(var Ni=0;Ni<vl.length;Ni++){var Ci=vl[Ni],Bf=Ci.toLowerCase(),Ff=Ci[0].toUpperCase()+Ci.slice(1);Wt(Bf,"on"+Ff)}Wt(gd,"onAnimationEnd");Wt(hd,"onAnimationIteration");Wt(xd,"onAnimationStart");Wt("dblclick","onDoubleClick");Wt("focusin","onFocus");Wt("focusout","onBlur");Wt(vd,"onTransitionEnd");Ln("onMouseEnter",["mouseout","mouseover"]);Ln("onMouseLeave",["mouseout","mouseover"]);Ln("onPointerEnter",["pointerout","pointerover"]);Ln("onPointerLeave",["pointerout","pointerover"]);on("onChange","change click focusin focusout input keydown keyup selectionchange".split(" "));on("onSelect","focusout contextmenu dragend focusin keydown keyup mousedown mouseup selectionchange".split(" "));on("onBeforeInput",["compositionend","keypress","textInput","paste"]);on("onCompositionEnd","compositionend focusout keydown keypress keyup mousedown".split(" "));on("onCompositionStart","compositionstart focusout keydown keypress keyup mousedown".split(" "));on("onCompositionUpdate","compositionupdate focusout keydown keypress keyup mousedown".split(" "));var sr="abort canplay canplaythrough durationchange emptied encrypted ended error loadeddata loadedmetadata loadstart pause play playing progress ratechange resize seeked seeking stalled suspend timeupdate volumechange waiting".split(" "),Uf=new Set("cancel close invalid load scroll toggle".split(" ").concat(sr));function yl(e,t,n){var o=e.type||"unknown-event";e.currentTarget=n,Op(o,t,void 0,e),e.currentTarget=null}function bd(e,t){t=(t&4)!==0;for(var n=0;n<e.length;n++){var o=e[n],i=o.event;o=o.listeners;e:{var a=void 0;if(t)for(var s=o.length-1;0<=s;s--){var l=o[s],c=l.instance,d=l.currentTarget;if(l=l.listener,c!==a&&i.isPropagationStopped())break e;yl(i,l,d),a=c}else for(s=0;s<o.length;s++){if(l=o[s],c=l.instance,d=l.currentTarget,l=l.listener,c!==a&&i.isPropagationStopped())break e;yl(i,l,d),a=c}}}if(wo)throw e=oa,wo=!1,oa=null,e}function ne(e,t){var n=t[ma];n===void 0&&(n=t[ma]=new Set);var o=e+"__bubble";n.has(o)||(kd(t,e,2,!1),n.add(o))}function Ti(e,t,n){var o=0;t&&(o|=4),kd(n,e,o,t)}var Yr="_reactListening"+Math.random().toString(36).slice(2);function Cr(e){if(!e[Yr]){e[Yr]=!0,Ec.forEach(function(n){n!=="selectionchange"&&(Uf.has(n)||Ti(n,!1,e),Ti(n,!0,e))});var t=e.nodeType===9?e:e.ownerDocument;t===null||t[Yr]||(t[Yr]=!0,Ti("selectionchange",!1,t))}}function kd(e,t,n,o){switch(od(t)){case 1:var i=ef;break;case 4:i=tf;break;default:i=Ya}n=i.bind(null,t,n,e),i=void 0,!ra||t!=="touchstart"&&t!=="touchmove"&&t!=="wheel"||(i=!0),o?i!==void 0?e.addEventListener(t,n,{capture:!0,passive:i}):e.addEventListener(t,n,!0):i!==void 0?e.addEventListener(t,n,{passive:i}):e.addEventListener(t,n,!1)}function Ei(e,t,n,o,i){var a=o;if(!(t&1)&&!(t&2)&&o!==null)e:for(;;){if(o===null)return;var s=o.tag;if(s===3||s===4){var l=o.stateNode.containerInfo;if(l===i||l.nodeType===8&&l.parentNode===i)break;if(s===4)for(s=o.return;s!==null;){var c=s.tag;if((c===3||c===4)&&(c=s.stateNode.containerInfo,c===i||c.nodeType===8&&c.parentNode===i))return;s=s.return}for(;l!==null;){if(s=Xt(l),s===null)return;if(c=s.tag,c===5||c===6){o=a=s;continue e}l=l.parentNode}}o=o.return}Hc(function(){var d=a,h=Va(n),f=[];e:{var g=yd.get(e);if(g!==void 0){var y=Za,v=e;switch(e){case"keypress":if(fo(n)===0)break e;case"keydown":case"keyup":y=xf;break;case"focusin":v="focus",y=ki;break;case"focusout":v="blur",y=ki;break;case"beforeblur":case"afterblur":y=ki;break;case"click":if(n.button===2)break e;case"auxclick":case"dblclick":case"mousedown":case"mousemove":case"mouseup":case"mouseout":case"mouseover":case"contextmenu":y=sl;break;case"drag":case"dragend":case"dragenter":case"dragexit":case"dragleave":case"dragover":case"dragstart":case"drop":y=of;break;case"touchcancel":case"touchend":case"touchmove":case"touchstart":y=bf;break;case gd:case hd:case xd:y=lf;break;case vd:y=jf;break;case"scroll":y=nf;break;case"wheel":y=Sf;break;case"copy":case"cut":case"paste":y=df;break;case"gotpointercapture":case"lostpointercapture":case"pointercancel":case"pointerdown":case"pointermove":case"pointerout":case"pointerover":case"pointerup":y=cl}var b=(t&4)!==0,z=!b&&e==="scroll",p=b?g!==null?g+"Capture":null:g;b=[];for(var u=d,m;u!==null;){m=u;var j=m.stateNode;if(m.tag===5&&j!==null&&(m=j,p!==null&&(j=br(u,p),j!=null&&b.push(Tr(u,j,m)))),z)break;u=u.return}0<b.length&&(g=new y(g,v,null,n,h),f.push({event:g,listeners:b}))}}if(!(t&7)){e:{if(g=e==="mouseover"||e==="pointerover",y=e==="mouseout"||e==="pointerout",g&&n!==ta&&(v=n.relatedTarget||n.fromElement)&&(Xt(v)||v[bt]))break e;if((y||g)&&(g=h.window===h?h:(g=h.ownerDocument)?g.defaultView||g.parentWindow:window,y?(v=n.relatedTarget||n.toElement,y=d,v=v?Xt(v):null,v!==null&&(z=an(v),v!==z||v.tag!==5&&v.tag!==6)&&(v=null)):(y=null,v=d),y!==v)){if(b=sl,j="onMouseLeave",p="onMouseEnter",u="mouse",(e==="pointerout"||e==="pointerover")&&(b=cl,j="onPointerLeave",p="onPointerEnter",u="pointer"),z=y==null?g:yn(y),m=v==null?g:yn(v),g=new b(j,u+"leave",y,n,h),g.target=z,g.relatedTarget=m,j=null,Xt(h)===d&&(b=new b(p,u+"enter",v,n,h),b.target=m,b.relatedTarget=z,j=b),z=j,y&&v)t:{for(b=y,p=v,u=0,m=b;m;m=un(m))u++;for(m=0,j=p;j;j=un(j))m++;for(;0<u-m;)b=un(b),u--;for(;0<m-u;)p=un(p),m--;for(;u--;){if(b===p||p!==null&&b===p.alternate)break t;b=un(b),p=un(p)}b=null}else b=null;y!==null&&bl(f,g,y,b,!1),v!==null&&z!==null&&bl(f,z,v,b,!0)}}e:{if(g=d?yn(d):window,y=g.nodeName&&g.nodeName.toLowerCase(),y==="select"||y==="input"&&g.type==="file")var T=Rf;else if(pl(g))if(dd)T=If;else{T=Pf;var w=Lf}else(y=g.nodeName)&&y.toLowerCase()==="input"&&(g.type==="checkbox"||g.type==="radio")&&(T=Df);if(T&&(T=T(e,d))){cd(f,T,n,h);break e}w&&w(e,g,d),e==="focusout"&&(w=g._wrapperState)&&w.controlled&&g.type==="number"&&Yi(g,"number",g.value)}switch(w=d?yn(d):window,e){case"focusin":(pl(w)||w.contentEditable==="true")&&(xn=w,la=d,fr=null);break;case"focusout":fr=la=xn=null;break;case"mousedown":ca=!0;break;case"contextmenu":case"mouseup":case"dragend":ca=!1,xl(f,n,h);break;case"selectionchange":if(Of)break;case"keydown":case"keyup":xl(f,n,h)}var C;if(es)e:{switch(e){case"compositionstart":var _="onCompositionStart";break e;case"compositionend":_="onCompositionEnd";break e;case"compositionupdate":_="onCompositionUpdate";break e}_=void 0}else hn?sd(e,n)&&(_="onCompositionEnd"):e==="keydown"&&n.keyCode===229&&(_="onCompositionStart");_&&(ad&&n.locale!=="ko"&&(hn||_!=="onCompositionStart"?_==="onCompositionEnd"&&hn&&(C=id()):(_t=h,Ja="value"in _t?_t.value:_t.textContent,hn=!0)),w=Eo(d,_),0<w.length&&(_=new ll(_,e,null,n,h),f.push({event:_,listeners:w}),C?_.data=C:(C=ld(n),C!==null&&(_.data=C)))),(C=Cf?Tf(e,n):Ef(e,n))&&(d=Eo(d,"onBeforeInput"),0<d.length&&(h=new ll("onBeforeInput","beforeinput",null,n,h),f.push({event:h,listeners:d}),h.data=C))}bd(f,t)})}function Tr(e,t,n){return{instance:e,listener:t,currentTarget:n}}function Eo(e,t){for(var n=t+"Capture",o=[];e!==null;){var i=e,a=i.stateNode;i.tag===5&&a!==null&&(i=a,a=br(e,n),a!=null&&o.unshift(Tr(e,a,i)),a=br(e,t),a!=null&&o.push(Tr(e,a,i))),e=e.return}return o}function un(e){if(e===null)return null;do e=e.return;while(e&&e.tag!==5);return e||null}function bl(e,t,n,o,i){for(var a=t._reactName,s=[];n!==null&&n!==o;){var l=n,c=l.alternate,d=l.stateNode;if(c!==null&&c===o)break;l.tag===5&&d!==null&&(l=d,i?(c=br(n,a),c!=null&&s.unshift(Tr(n,c,l))):i||(c=br(n,a),c!=null&&s.push(Tr(n,c,l)))),n=n.return}s.length!==0&&e.push({event:t,listeners:s})}var Wf=/\r\n?/g,$f=/\u0000|\uFFFD/g;function kl(e){return(typeof e=="string"?e:""+e).replace(Wf,`
`).replace($f,"")}function Jr(e,t,n){if(t=kl(t),kl(e)!==t&&n)throw Error(L(425))}function zo(){}var da=null,ua=null;function pa(e,t){return e==="textarea"||e==="noscript"||typeof t.children=="string"||typeof t.children=="number"||typeof t.dangerouslySetInnerHTML=="object"&&t.dangerouslySetInnerHTML!==null&&t.dangerouslySetInnerHTML.__html!=null}var fa=typeof setTimeout=="function"?setTimeout:void 0,Hf=typeof clearTimeout=="function"?clearTimeout:void 0,jl=typeof Promise=="function"?Promise:void 0,Kf=typeof queueMicrotask=="function"?queueMicrotask:typeof jl<"u"?function(e){return jl.resolve(null).then(e).catch(Vf)}:fa;function Vf(e){setTimeout(function(){throw e})}function zi(e,t){var n=t,o=0;do{var i=n.nextSibling;if(e.removeChild(n),i&&i.nodeType===8)if(n=i.data,n==="/$"){if(o===0){e.removeChild(i),wr(t);return}o--}else n!=="$"&&n!=="$?"&&n!=="$!"||o++;n=i}while(n);wr(t)}function It(e){for(;e!=null;e=e.nextSibling){var t=e.nodeType;if(t===1||t===3)break;if(t===8){if(t=e.data,t==="$"||t==="$!"||t==="$?")break;if(t==="/$")return null}}return e}function wl(e){e=e.previousSibling;for(var t=0;e;){if(e.nodeType===8){var n=e.data;if(n==="$"||n==="$!"||n==="$?"){if(t===0)return e;t--}else n==="/$"&&t++}e=e.previousSibling}return null}var Un=Math.random().toString(36).slice(2),dt="__reactFiber$"+Un,Er="__reactProps$"+Un,bt="__reactContainer$"+Un,ma="__reactEvents$"+Un,Gf="__reactListeners$"+Un,Xf="__reactHandles$"+Un;function Xt(e){var t=e[dt];if(t)return t;for(var n=e.parentNode;n;){if(t=n[bt]||n[dt]){if(n=t.alternate,t.child!==null||n!==null&&n.child!==null)for(e=wl(e);e!==null;){if(n=e[dt])return n;e=wl(e)}return t}e=n,n=e.parentNode}return null}function Or(e){return e=e[dt]||e[bt],!e||e.tag!==5&&e.tag!==6&&e.tag!==13&&e.tag!==3?null:e}function yn(e){if(e.tag===5||e.tag===6)return e.stateNode;throw Error(L(33))}function Jo(e){return e[Er]||null}var ga=[],bn=-1;function $t(e){return{current:e}}function re(e){0>bn||(e.current=ga[bn],ga[bn]=null,bn--)}function ee(e,t){bn++,ga[bn]=e.current,e.current=t}var Ut={},Te=$t(Ut),Ae=$t(!1),qt=Ut;function Pn(e,t){var n=e.type.contextTypes;if(!n)return Ut;var o=e.stateNode;if(o&&o.__reactInternalMemoizedUnmaskedChildContext===t)return o.__reactInternalMemoizedMaskedChildContext;var i={},a;for(a in n)i[a]=t[a];return o&&(e=e.stateNode,e.__reactInternalMemoizedUnmaskedChildContext=t,e.__reactInternalMemoizedMaskedChildContext=i),i}function Oe(e){return e=e.childContextTypes,e!=null}function _o(){re(Ae),re(Te)}function Sl(e,t,n){if(Te.current!==Ut)throw Error(L(168));ee(Te,t),ee(Ae,n)}function jd(e,t,n){var o=e.stateNode;if(t=t.childContextTypes,typeof o.getChildContext!="function")return n;o=o.getChildContext();for(var i in o)if(!(i in t))throw Error(L(108,Rp(e)||"Unknown",i));return se({},n,o)}function Ro(e){return e=(e=e.stateNode)&&e.__reactInternalMemoizedMergedChildContext||Ut,qt=Te.current,ee(Te,e),ee(Ae,Ae.current),!0}function Nl(e,t,n){var o=e.stateNode;if(!o)throw Error(L(169));n?(e=jd(e,t,qt),o.__reactInternalMemoizedMergedChildContext=e,re(Ae),re(Te),ee(Te,e)):re(Ae),ee(Ae,n)}var gt=null,Zo=!1,_i=!1;function wd(e){gt===null?gt=[e]:gt.push(e)}function Qf(e){Zo=!0,wd(e)}function Ht(){if(!_i&&gt!==null){_i=!0;var e=0,t=Z;try{var n=gt;for(Z=1;e<n.length;e++){var o=n[e];do o=o(!0);while(o!==null)}gt=null,Zo=!1}catch(i){throw gt!==null&&(gt=gt.slice(e+1)),Xc(Ga,Ht),i}finally{Z=t,_i=!1}}return null}var kn=[],jn=0,Lo=null,Po=0,Xe=[],Qe=0,en=null,ht=1,xt="";function Vt(e,t){kn[jn++]=Po,kn[jn++]=Lo,Lo=e,Po=t}function Sd(e,t,n){Xe[Qe++]=ht,Xe[Qe++]=xt,Xe[Qe++]=en,en=e;var o=ht;e=xt;var i=32-it(o)-1;o&=~(1<<i),n+=1;var a=32-it(t)+i;if(30<a){var s=i-i%5;a=(o&(1<<s)-1).toString(32),o>>=s,i-=s,ht=1<<32-it(t)+i|n<<i|o,xt=a+e}else ht=1<<a|n<<i|o,xt=e}function ns(e){e.return!==null&&(Vt(e,1),Sd(e,1,0))}function rs(e){for(;e===Lo;)Lo=kn[--jn],kn[jn]=null,Po=kn[--jn],kn[jn]=null;for(;e===en;)en=Xe[--Qe],Xe[Qe]=null,xt=Xe[--Qe],Xe[Qe]=null,ht=Xe[--Qe],Xe[Qe]=null}var $e=null,We=null,oe=!1,ot=null;function Nd(e,t){var n=Ye(5,null,null,0);n.elementType="DELETED",n.stateNode=t,n.return=e,t=e.deletions,t===null?(e.deletions=[n],e.flags|=16):t.push(n)}function Cl(e,t){switch(e.tag){case 5:var n=e.type;return t=t.nodeType!==1||n.toLowerCase()!==t.nodeName.toLowerCase()?null:t,t!==null?(e.stateNode=t,$e=e,We=It(t.firstChild),!0):!1;case 6:return t=e.pendingProps===""||t.nodeType!==3?null:t,t!==null?(e.stateNode=t,$e=e,We=null,!0):!1;case 13:return t=t.nodeType!==8?null:t,t!==null?(n=en!==null?{id:ht,overflow:xt}:null,e.memoizedState={dehydrated:t,treeContext:n,retryLane:1073741824},n=Ye(18,null,null,0),n.stateNode=t,n.return=e,e.child=n,$e=e,We=null,!0):!1;default:return!1}}function ha(e){return(e.mode&1)!==0&&(e.flags&128)===0}function xa(e){if(oe){var t=We;if(t){var n=t;if(!Cl(e,t)){if(ha(e))throw Error(L(418));t=It(n.nextSibling);var o=$e;t&&Cl(e,t)?Nd(o,n):(e.flags=e.flags&-4097|2,oe=!1,$e=e)}}else{if(ha(e))throw Error(L(418));e.flags=e.flags&-4097|2,oe=!1,$e=e}}}function Tl(e){for(e=e.return;e!==null&&e.tag!==5&&e.tag!==3&&e.tag!==13;)e=e.return;$e=e}function Zr(e){if(e!==$e)return!1;if(!oe)return Tl(e),oe=!0,!1;var t;if((t=e.tag!==3)&&!(t=e.tag!==5)&&(t=e.type,t=t!=="head"&&t!=="body"&&!pa(e.type,e.memoizedProps)),t&&(t=We)){if(ha(e))throw Cd(),Error(L(418));for(;t;)Nd(e,t),t=It(t.nextSibling)}if(Tl(e),e.tag===13){if(e=e.memoizedState,e=e!==null?e.dehydrated:null,!e)throw Error(L(317));e:{for(e=e.nextSibling,t=0;e;){if(e.nodeType===8){var n=e.data;if(n==="/$"){if(t===0){We=It(e.nextSibling);break e}t--}else n!=="$"&&n!=="$!"&&n!=="$?"||t++}e=e.nextSibling}We=null}}else We=$e?It(e.stateNode.nextSibling):null;return!0}function Cd(){for(var e=We;e;)e=It(e.nextSibling)}function Dn(){We=$e=null,oe=!1}function os(e){ot===null?ot=[e]:ot.push(e)}var Yf=wt.ReactCurrentBatchConfig;function qn(e,t,n){if(e=n.ref,e!==null&&typeof e!="function"&&typeof e!="object"){if(n._owner){if(n=n._owner,n){if(n.tag!==1)throw Error(L(309));var o=n.stateNode}if(!o)throw Error(L(147,e));var i=o,a=""+e;return t!==null&&t.ref!==null&&typeof t.ref=="function"&&t.ref._stringRef===a?t.ref:(t=function(s){var l=i.refs;s===null?delete l[a]:l[a]=s},t._stringRef=a,t)}if(typeof e!="string")throw Error(L(284));if(!n._owner)throw Error(L(290,e))}return e}function qr(e,t){throw e=Object.prototype.toString.call(t),Error(L(31,e==="[object Object]"?"object with keys {"+Object.keys(t).join(", ")+"}":e))}function El(e){var t=e._init;return t(e._payload)}function Td(e){function t(p,u){if(e){var m=p.deletions;m===null?(p.deletions=[u],p.flags|=16):m.push(u)}}function n(p,u){if(!e)return null;for(;u!==null;)t(p,u),u=u.sibling;return null}function o(p,u){for(p=new Map;u!==null;)u.key!==null?p.set(u.key,u):p.set(u.index,u),u=u.sibling;return p}function i(p,u){return p=Bt(p,u),p.index=0,p.sibling=null,p}function a(p,u,m){return p.index=m,e?(m=p.alternate,m!==null?(m=m.index,m<u?(p.flags|=2,u):m):(p.flags|=2,u)):(p.flags|=1048576,u)}function s(p){return e&&p.alternate===null&&(p.flags|=2),p}function l(p,u,m,j){return u===null||u.tag!==6?(u=Ai(m,p.mode,j),u.return=p,u):(u=i(u,m),u.return=p,u)}function c(p,u,m,j){var T=m.type;return T===gn?h(p,u,m.props.children,j,m.key):u!==null&&(u.elementType===T||typeof T=="object"&&T!==null&&T.$$typeof===Ct&&El(T)===u.type)?(j=i(u,m.props),j.ref=qn(p,u,m),j.return=p,j):(j=bo(m.type,m.key,m.props,null,p.mode,j),j.ref=qn(p,u,m),j.return=p,j)}function d(p,u,m,j){return u===null||u.tag!==4||u.stateNode.containerInfo!==m.containerInfo||u.stateNode.implementation!==m.implementation?(u=Oi(m,p.mode,j),u.return=p,u):(u=i(u,m.children||[]),u.return=p,u)}function h(p,u,m,j,T){return u===null||u.tag!==7?(u=Zt(m,p.mode,j,T),u.return=p,u):(u=i(u,m),u.return=p,u)}function f(p,u,m){if(typeof u=="string"&&u!==""||typeof u=="number")return u=Ai(""+u,p.mode,m),u.return=p,u;if(typeof u=="object"&&u!==null){switch(u.$$typeof){case Wr:return m=bo(u.type,u.key,u.props,null,p.mode,m),m.ref=qn(p,null,u),m.return=p,m;case mn:return u=Oi(u,p.mode,m),u.return=p,u;case Ct:var j=u._init;return f(p,j(u._payload),m)}if(ir(u)||Xn(u))return u=Zt(u,p.mode,m,null),u.return=p,u;qr(p,u)}return null}function g(p,u,m,j){var T=u!==null?u.key:null;if(typeof m=="string"&&m!==""||typeof m=="number")return T!==null?null:l(p,u,""+m,j);if(typeof m=="object"&&m!==null){switch(m.$$typeof){case Wr:return m.key===T?c(p,u,m,j):null;case mn:return m.key===T?d(p,u,m,j):null;case Ct:return T=m._init,g(p,u,T(m._payload),j)}if(ir(m)||Xn(m))return T!==null?null:h(p,u,m,j,null);qr(p,m)}return null}function y(p,u,m,j,T){if(typeof j=="string"&&j!==""||typeof j=="number")return p=p.get(m)||null,l(u,p,""+j,T);if(typeof j=="object"&&j!==null){switch(j.$$typeof){case Wr:return p=p.get(j.key===null?m:j.key)||null,c(u,p,j,T);case mn:return p=p.get(j.key===null?m:j.key)||null,d(u,p,j,T);case Ct:var w=j._init;return y(p,u,m,w(j._payload),T)}if(ir(j)||Xn(j))return p=p.get(m)||null,h(u,p,j,T,null);qr(u,j)}return null}function v(p,u,m,j){for(var T=null,w=null,C=u,_=u=0,P=null;C!==null&&_<m.length;_++){C.index>_?(P=C,C=null):P=C.sibling;var N=g(p,C,m[_],j);if(N===null){C===null&&(C=P);break}e&&C&&N.alternate===null&&t(p,C),u=a(N,u,_),w===null?T=N:w.sibling=N,w=N,C=P}if(_===m.length)return n(p,C),oe&&Vt(p,_),T;if(C===null){for(;_<m.length;_++)C=f(p,m[_],j),C!==null&&(u=a(C,u,_),w===null?T=C:w.sibling=C,w=C);return oe&&Vt(p,_),T}for(C=o(p,C);_<m.length;_++)P=y(C,p,_,m[_],j),P!==null&&(e&&P.alternate!==null&&C.delete(P.key===null?_:P.key),u=a(P,u,_),w===null?T=P:w.sibling=P,w=P);return e&&C.forEach(function(B){return t(p,B)}),oe&&Vt(p,_),T}function b(p,u,m,j){var T=Xn(m);if(typeof T!="function")throw Error(L(150));if(m=T.call(m),m==null)throw Error(L(151));for(var w=T=null,C=u,_=u=0,P=null,N=m.next();C!==null&&!N.done;_++,N=m.next()){C.index>_?(P=C,C=null):P=C.sibling;var B=g(p,C,N.value,j);if(B===null){C===null&&(C=P);break}e&&C&&B.alternate===null&&t(p,C),u=a(B,u,_),w===null?T=B:w.sibling=B,w=B,C=P}if(N.done)return n(p,C),oe&&Vt(p,_),T;if(C===null){for(;!N.done;_++,N=m.next())N=f(p,N.value,j),N!==null&&(u=a(N,u,_),w===null?T=N:w.sibling=N,w=N);return oe&&Vt(p,_),T}for(C=o(p,C);!N.done;_++,N=m.next())N=y(C,p,_,N.value,j),N!==null&&(e&&N.alternate!==null&&C.delete(N.key===null?_:N.key),u=a(N,u,_),w===null?T=N:w.sibling=N,w=N);return e&&C.forEach(function(K){return t(p,K)}),oe&&Vt(p,_),T}function z(p,u,m,j){if(typeof m=="object"&&m!==null&&m.type===gn&&m.key===null&&(m=m.props.children),typeof m=="object"&&m!==null){switch(m.$$typeof){case Wr:e:{for(var T=m.key,w=u;w!==null;){if(w.key===T){if(T=m.type,T===gn){if(w.tag===7){n(p,w.sibling),u=i(w,m.props.children),u.return=p,p=u;break e}}else if(w.elementType===T||typeof T=="object"&&T!==null&&T.$$typeof===Ct&&El(T)===w.type){n(p,w.sibling),u=i(w,m.props),u.ref=qn(p,w,m),u.return=p,p=u;break e}n(p,w);break}else t(p,w);w=w.sibling}m.type===gn?(u=Zt(m.props.children,p.mode,j,m.key),u.return=p,p=u):(j=bo(m.type,m.key,m.props,null,p.mode,j),j.ref=qn(p,u,m),j.return=p,p=j)}return s(p);case mn:e:{for(w=m.key;u!==null;){if(u.key===w)if(u.tag===4&&u.stateNode.containerInfo===m.containerInfo&&u.stateNode.implementation===m.implementation){n(p,u.sibling),u=i(u,m.children||[]),u.return=p,p=u;break e}else{n(p,u);break}else t(p,u);u=u.sibling}u=Oi(m,p.mode,j),u.return=p,p=u}return s(p);case Ct:return w=m._init,z(p,u,w(m._payload),j)}if(ir(m))return v(p,u,m,j);if(Xn(m))return b(p,u,m,j);qr(p,m)}return typeof m=="string"&&m!==""||typeof m=="number"?(m=""+m,u!==null&&u.tag===6?(n(p,u.sibling),u=i(u,m),u.return=p,p=u):(n(p,u),u=Ai(m,p.mode,j),u.return=p,p=u),s(p)):n(p,u)}return z}var In=Td(!0),Ed=Td(!1),Do=$t(null),Io=null,wn=null,is=null;function as(){is=wn=Io=null}function ss(e){var t=Do.current;re(Do),e._currentValue=t}function va(e,t,n){for(;e!==null;){var o=e.alternate;if((e.childLanes&t)!==t?(e.childLanes|=t,o!==null&&(o.childLanes|=t)):o!==null&&(o.childLanes&t)!==t&&(o.childLanes|=t),e===n)break;e=e.return}}function _n(e,t){Io=e,is=wn=null,e=e.dependencies,e!==null&&e.firstContext!==null&&(e.lanes&t&&(Me=!0),e.firstContext=null)}function Ze(e){var t=e._currentValue;if(is!==e)if(e={context:e,memoizedValue:t,next:null},wn===null){if(Io===null)throw Error(L(308));wn=e,Io.dependencies={lanes:0,firstContext:e}}else wn=wn.next=e;return t}var Qt=null;function ls(e){Qt===null?Qt=[e]:Qt.push(e)}function zd(e,t,n,o){var i=t.interleaved;return i===null?(n.next=n,ls(t)):(n.next=i.next,i.next=n),t.interleaved=n,kt(e,o)}function kt(e,t){e.lanes|=t;var n=e.alternate;for(n!==null&&(n.lanes|=t),n=e,e=e.return;e!==null;)e.childLanes|=t,n=e.alternate,n!==null&&(n.childLanes|=t),n=e,e=e.return;return n.tag===3?n.stateNode:null}var Tt=!1;function cs(e){e.updateQueue={baseState:e.memoizedState,firstBaseUpdate:null,lastBaseUpdate:null,shared:{pending:null,interleaved:null,lanes:0},effects:null}}function _d(e,t){e=e.updateQueue,t.updateQueue===e&&(t.updateQueue={baseState:e.baseState,firstBaseUpdate:e.firstBaseUpdate,lastBaseUpdate:e.lastBaseUpdate,shared:e.shared,effects:e.effects})}function vt(e,t){return{eventTime:e,lane:t,tag:0,payload:null,callback:null,next:null}}function Mt(e,t,n){var o=e.updateQueue;if(o===null)return null;if(o=o.shared,X&2){var i=o.pending;return i===null?t.next=t:(t.next=i.next,i.next=t),o.pending=t,kt(e,n)}return i=o.interleaved,i===null?(t.next=t,ls(o)):(t.next=i.next,i.next=t),o.interleaved=t,kt(e,n)}function mo(e,t,n){if(t=t.updateQueue,t!==null&&(t=t.shared,(n&4194240)!==0)){var o=t.lanes;o&=e.pendingLanes,n|=o,t.lanes=n,Xa(e,n)}}function zl(e,t){var n=e.updateQueue,o=e.alternate;if(o!==null&&(o=o.updateQueue,n===o)){var i=null,a=null;if(n=n.firstBaseUpdate,n!==null){do{var s={eventTime:n.eventTime,lane:n.lane,tag:n.tag,payload:n.payload,callback:n.callback,next:null};a===null?i=a=s:a=a.next=s,n=n.next}while(n!==null);a===null?i=a=t:a=a.next=t}else i=a=t;n={baseState:o.baseState,firstBaseUpdate:i,lastBaseUpdate:a,shared:o.shared,effects:o.effects},e.updateQueue=n;return}e=n.lastBaseUpdate,e===null?n.firstBaseUpdate=t:e.next=t,n.lastBaseUpdate=t}function Mo(e,t,n,o){var i=e.updateQueue;Tt=!1;var a=i.firstBaseUpdate,s=i.lastBaseUpdate,l=i.shared.pending;if(l!==null){i.shared.pending=null;var c=l,d=c.next;c.next=null,s===null?a=d:s.next=d,s=c;var h=e.alternate;h!==null&&(h=h.updateQueue,l=h.lastBaseUpdate,l!==s&&(l===null?h.firstBaseUpdate=d:l.next=d,h.lastBaseUpdate=c))}if(a!==null){var f=i.baseState;s=0,h=d=c=null,l=a;do{var g=l.lane,y=l.eventTime;if((o&g)===g){h!==null&&(h=h.next={eventTime:y,lane:0,tag:l.tag,payload:l.payload,callback:l.callback,next:null});e:{var v=e,b=l;switch(g=t,y=n,b.tag){case 1:if(v=b.payload,typeof v=="function"){f=v.call(y,f,g);break e}f=v;break e;case 3:v.flags=v.flags&-65537|128;case 0:if(v=b.payload,g=typeof v=="function"?v.call(y,f,g):v,g==null)break e;f=se({},f,g);break e;case 2:Tt=!0}}l.callback!==null&&l.lane!==0&&(e.flags|=64,g=i.effects,g===null?i.effects=[l]:g.push(l))}else y={eventTime:y,lane:g,tag:l.tag,payload:l.payload,callback:l.callback,next:null},h===null?(d=h=y,c=f):h=h.next=y,s|=g;if(l=l.next,l===null){if(l=i.shared.pending,l===null)break;g=l,l=g.next,g.next=null,i.lastBaseUpdate=g,i.shared.pending=null}}while(!0);if(h===null&&(c=f),i.baseState=c,i.firstBaseUpdate=d,i.lastBaseUpdate=h,t=i.shared.interleaved,t!==null){i=t;do s|=i.lane,i=i.next;while(i!==t)}else a===null&&(i.shared.lanes=0);nn|=s,e.lanes=s,e.memoizedState=f}}function _l(e,t,n){if(e=t.effects,t.effects=null,e!==null)for(t=0;t<e.length;t++){var o=e[t],i=o.callback;if(i!==null){if(o.callback=null,o=n,typeof i!="function")throw Error(L(191,i));i.call(o)}}}var Br={},pt=$t(Br),zr=$t(Br),_r=$t(Br);function Yt(e){if(e===Br)throw Error(L(174));return e}function ds(e,t){switch(ee(_r,t),ee(zr,e),ee(pt,Br),e=t.nodeType,e){case 9:case 11:t=(t=t.documentElement)?t.namespaceURI:Zi(null,"");break;default:e=e===8?t.parentNode:t,t=e.namespaceURI||null,e=e.tagName,t=Zi(t,e)}re(pt),ee(pt,t)}function Mn(){re(pt),re(zr),re(_r)}function Rd(e){Yt(_r.current);var t=Yt(pt.current),n=Zi(t,e.type);t!==n&&(ee(zr,e),ee(pt,n))}function us(e){zr.current===e&&(re(pt),re(zr))}var ie=$t(0);function Ao(e){for(var t=e;t!==null;){if(t.tag===13){var n=t.memoizedState;if(n!==null&&(n=n.dehydrated,n===null||n.data==="$?"||n.data==="$!"))return t}else if(t.tag===19&&t.memoizedProps.revealOrder!==void 0){if(t.flags&128)return t}else if(t.child!==null){t.child.return=t,t=t.child;continue}if(t===e)break;for(;t.sibling===null;){if(t.return===null||t.return===e)return null;t=t.return}t.sibling.return=t.return,t=t.sibling}return null}var Ri=[];function ps(){for(var e=0;e<Ri.length;e++)Ri[e]._workInProgressVersionPrimary=null;Ri.length=0}var go=wt.ReactCurrentDispatcher,Li=wt.ReactCurrentBatchConfig,tn=0,ae=null,me=null,xe=null,Oo=!1,mr=!1,Rr=0,Jf=0;function Se(){throw Error(L(321))}function fs(e,t){if(t===null)return!1;for(var n=0;n<t.length&&n<e.length;n++)if(!st(e[n],t[n]))return!1;return!0}function ms(e,t,n,o,i,a){if(tn=a,ae=t,t.memoizedState=null,t.updateQueue=null,t.lanes=0,go.current=e===null||e.memoizedState===null?tm:nm,e=n(o,i),mr){a=0;do{if(mr=!1,Rr=0,25<=a)throw Error(L(301));a+=1,xe=me=null,t.updateQueue=null,go.current=rm,e=n(o,i)}while(mr)}if(go.current=Bo,t=me!==null&&me.next!==null,tn=0,xe=me=ae=null,Oo=!1,t)throw Error(L(300));return e}function gs(){var e=Rr!==0;return Rr=0,e}function ct(){var e={memoizedState:null,baseState:null,baseQueue:null,queue:null,next:null};return xe===null?ae.memoizedState=xe=e:xe=xe.next=e,xe}function qe(){if(me===null){var e=ae.alternate;e=e!==null?e.memoizedState:null}else e=me.next;var t=xe===null?ae.memoizedState:xe.next;if(t!==null)xe=t,me=e;else{if(e===null)throw Error(L(310));me=e,e={memoizedState:me.memoizedState,baseState:me.baseState,baseQueue:me.baseQueue,queue:me.queue,next:null},xe===null?ae.memoizedState=xe=e:xe=xe.next=e}return xe}function Lr(e,t){return typeof t=="function"?t(e):t}function Pi(e){var t=qe(),n=t.queue;if(n===null)throw Error(L(311));n.lastRenderedReducer=e;var o=me,i=o.baseQueue,a=n.pending;if(a!==null){if(i!==null){var s=i.next;i.next=a.next,a.next=s}o.baseQueue=i=a,n.pending=null}if(i!==null){a=i.next,o=o.baseState;var l=s=null,c=null,d=a;do{var h=d.lane;if((tn&h)===h)c!==null&&(c=c.next={lane:0,action:d.action,hasEagerState:d.hasEagerState,eagerState:d.eagerState,next:null}),o=d.hasEagerState?d.eagerState:e(o,d.action);else{var f={lane:h,action:d.action,hasEagerState:d.hasEagerState,eagerState:d.eagerState,next:null};c===null?(l=c=f,s=o):c=c.next=f,ae.lanes|=h,nn|=h}d=d.next}while(d!==null&&d!==a);c===null?s=o:c.next=l,st(o,t.memoizedState)||(Me=!0),t.memoizedState=o,t.baseState=s,t.baseQueue=c,n.lastRenderedState=o}if(e=n.interleaved,e!==null){i=e;do a=i.lane,ae.lanes|=a,nn|=a,i=i.next;while(i!==e)}else i===null&&(n.lanes=0);return[t.memoizedState,n.dispatch]}function Di(e){var t=qe(),n=t.queue;if(n===null)throw Error(L(311));n.lastRenderedReducer=e;var o=n.dispatch,i=n.pending,a=t.memoizedState;if(i!==null){n.pending=null;var s=i=i.next;do a=e(a,s.action),s=s.next;while(s!==i);st(a,t.memoizedState)||(Me=!0),t.memoizedState=a,t.baseQueue===null&&(t.baseState=a),n.lastRenderedState=a}return[a,o]}function Ld(){}function Pd(e,t){var n=ae,o=qe(),i=t(),a=!st(o.memoizedState,i);if(a&&(o.memoizedState=i,Me=!0),o=o.queue,hs(Md.bind(null,n,o,e),[e]),o.getSnapshot!==t||a||xe!==null&&xe.memoizedState.tag&1){if(n.flags|=2048,Pr(9,Id.bind(null,n,o,i,t),void 0,null),ve===null)throw Error(L(349));tn&30||Dd(n,t,i)}return i}function Dd(e,t,n){e.flags|=16384,e={getSnapshot:t,value:n},t=ae.updateQueue,t===null?(t={lastEffect:null,stores:null},ae.updateQueue=t,t.stores=[e]):(n=t.stores,n===null?t.stores=[e]:n.push(e))}function Id(e,t,n,o){t.value=n,t.getSnapshot=o,Ad(t)&&Od(e)}function Md(e,t,n){return n(function(){Ad(t)&&Od(e)})}function Ad(e){var t=e.getSnapshot;e=e.value;try{var n=t();return!st(e,n)}catch{return!0}}function Od(e){var t=kt(e,1);t!==null&&at(t,e,1,-1)}function Rl(e){var t=ct();return typeof e=="function"&&(e=e()),t.memoizedState=t.baseState=e,e={pending:null,interleaved:null,lanes:0,dispatch:null,lastRenderedReducer:Lr,lastRenderedState:e},t.queue=e,e=e.dispatch=em.bind(null,ae,e),[t.memoizedState,e]}function Pr(e,t,n,o){return e={tag:e,create:t,destroy:n,deps:o,next:null},t=ae.updateQueue,t===null?(t={lastEffect:null,stores:null},ae.updateQueue=t,t.lastEffect=e.next=e):(n=t.lastEffect,n===null?t.lastEffect=e.next=e:(o=n.next,n.next=e,e.next=o,t.lastEffect=e)),e}function Bd(){return qe().memoizedState}function ho(e,t,n,o){var i=ct();ae.flags|=e,i.memoizedState=Pr(1|t,n,void 0,o===void 0?null:o)}function qo(e,t,n,o){var i=qe();o=o===void 0?null:o;var a=void 0;if(me!==null){var s=me.memoizedState;if(a=s.destroy,o!==null&&fs(o,s.deps)){i.memoizedState=Pr(t,n,a,o);return}}ae.flags|=e,i.memoizedState=Pr(1|t,n,a,o)}function Ll(e,t){return ho(8390656,8,e,t)}function hs(e,t){return qo(2048,8,e,t)}function Fd(e,t){return qo(4,2,e,t)}function Ud(e,t){return qo(4,4,e,t)}function Wd(e,t){if(typeof t=="function")return e=e(),t(e),function(){t(null)};if(t!=null)return e=e(),t.current=e,function(){t.current=null}}function $d(e,t,n){return n=n!=null?n.concat([e]):null,qo(4,4,Wd.bind(null,t,e),n)}function xs(){}function Hd(e,t){var n=qe();t=t===void 0?null:t;var o=n.memoizedState;return o!==null&&t!==null&&fs(t,o[1])?o[0]:(n.memoizedState=[e,t],e)}function Kd(e,t){var n=qe();t=t===void 0?null:t;var o=n.memoizedState;return o!==null&&t!==null&&fs(t,o[1])?o[0]:(e=e(),n.memoizedState=[e,t],e)}function Vd(e,t,n){return tn&21?(st(n,t)||(n=Jc(),ae.lanes|=n,nn|=n,e.baseState=!0),t):(e.baseState&&(e.baseState=!1,Me=!0),e.memoizedState=n)}function Zf(e,t){var n=Z;Z=n!==0&&4>n?n:4,e(!0);var o=Li.transition;Li.transition={};try{e(!1),t()}finally{Z=n,Li.transition=o}}function Gd(){return qe().memoizedState}function qf(e,t,n){var o=Ot(e);if(n={lane:o,action:n,hasEagerState:!1,eagerState:null,next:null},Xd(e))Qd(t,n);else if(n=zd(e,t,n,o),n!==null){var i=_e();at(n,e,o,i),Yd(n,t,o)}}function em(e,t,n){var o=Ot(e),i={lane:o,action:n,hasEagerState:!1,eagerState:null,next:null};if(Xd(e))Qd(t,i);else{var a=e.alternate;if(e.lanes===0&&(a===null||a.lanes===0)&&(a=t.lastRenderedReducer,a!==null))try{var s=t.lastRenderedState,l=a(s,n);if(i.hasEagerState=!0,i.eagerState=l,st(l,s)){var c=t.interleaved;c===null?(i.next=i,ls(t)):(i.next=c.next,c.next=i),t.interleaved=i;return}}catch{}finally{}n=zd(e,t,i,o),n!==null&&(i=_e(),at(n,e,o,i),Yd(n,t,o))}}function Xd(e){var t=e.alternate;return e===ae||t!==null&&t===ae}function Qd(e,t){mr=Oo=!0;var n=e.pending;n===null?t.next=t:(t.next=n.next,n.next=t),e.pending=t}function Yd(e,t,n){if(n&4194240){var o=t.lanes;o&=e.pendingLanes,n|=o,t.lanes=n,Xa(e,n)}}var Bo={readContext:Ze,useCallback:Se,useContext:Se,useEffect:Se,useImperativeHandle:Se,useInsertionEffect:Se,useLayoutEffect:Se,useMemo:Se,useReducer:Se,useRef:Se,useState:Se,useDebugValue:Se,useDeferredValue:Se,useTransition:Se,useMutableSource:Se,useSyncExternalStore:Se,useId:Se,unstable_isNewReconciler:!1},tm={readContext:Ze,useCallback:function(e,t){return ct().memoizedState=[e,t===void 0?null:t],e},useContext:Ze,useEffect:Ll,useImperativeHandle:function(e,t,n){return n=n!=null?n.concat([e]):null,ho(4194308,4,Wd.bind(null,t,e),n)},useLayoutEffect:function(e,t){return ho(4194308,4,e,t)},useInsertionEffect:function(e,t){return ho(4,2,e,t)},useMemo:function(e,t){var n=ct();return t=t===void 0?null:t,e=e(),n.memoizedState=[e,t],e},useReducer:function(e,t,n){var o=ct();return t=n!==void 0?n(t):t,o.memoizedState=o.baseState=t,e={pending:null,interleaved:null,lanes:0,dispatch:null,lastRenderedReducer:e,lastRenderedState:t},o.queue=e,e=e.dispatch=qf.bind(null,ae,e),[o.memoizedState,e]},useRef:function(e){var t=ct();return e={current:e},t.memoizedState=e},useState:Rl,useDebugValue:xs,useDeferredValue:function(e){return ct().memoizedState=e},useTransition:function(){var e=Rl(!1),t=e[0];return e=Zf.bind(null,e[1]),ct().memoizedState=e,[t,e]},useMutableSource:function(){},useSyncExternalStore:function(e,t,n){var o=ae,i=ct();if(oe){if(n===void 0)throw Error(L(407));n=n()}else{if(n=t(),ve===null)throw Error(L(349));tn&30||Dd(o,t,n)}i.memoizedState=n;var a={value:n,getSnapshot:t};return i.queue=a,Ll(Md.bind(null,o,a,e),[e]),o.flags|=2048,Pr(9,Id.bind(null,o,a,n,t),void 0,null),n},useId:function(){var e=ct(),t=ve.identifierPrefix;if(oe){var n=xt,o=ht;n=(o&~(1<<32-it(o)-1)).toString(32)+n,t=":"+t+"R"+n,n=Rr++,0<n&&(t+="H"+n.toString(32)),t+=":"}else n=Jf++,t=":"+t+"r"+n.toString(32)+":";return e.memoizedState=t},unstable_isNewReconciler:!1},nm={readContext:Ze,useCallback:Hd,useContext:Ze,useEffect:hs,useImperativeHandle:$d,useInsertionEffect:Fd,useLayoutEffect:Ud,useMemo:Kd,useReducer:Pi,useRef:Bd,useState:function(){return Pi(Lr)},useDebugValue:xs,useDeferredValue:function(e){var t=qe();return Vd(t,me.memoizedState,e)},useTransition:function(){var e=Pi(Lr)[0],t=qe().memoizedState;return[e,t]},useMutableSource:Ld,useSyncExternalStore:Pd,useId:Gd,unstable_isNewReconciler:!1},rm={readContext:Ze,useCallback:Hd,useContext:Ze,useEffect:hs,useImperativeHandle:$d,useInsertionEffect:Fd,useLayoutEffect:Ud,useMemo:Kd,useReducer:Di,useRef:Bd,useState:function(){return Di(Lr)},useDebugValue:xs,useDeferredValue:function(e){var t=qe();return me===null?t.memoizedState=e:Vd(t,me.memoizedState,e)},useTransition:function(){var e=Di(Lr)[0],t=qe().memoizedState;return[e,t]},useMutableSource:Ld,useSyncExternalStore:Pd,useId:Gd,unstable_isNewReconciler:!1};function nt(e,t){if(e&&e.defaultProps){t=se({},t),e=e.defaultProps;for(var n in e)t[n]===void 0&&(t[n]=e[n]);return t}return t}function ya(e,t,n,o){t=e.memoizedState,n=n(o,t),n=n==null?t:se({},t,n),e.memoizedState=n,e.lanes===0&&(e.updateQueue.baseState=n)}var ei={isMounted:function(e){return(e=e._reactInternals)?an(e)===e:!1},enqueueSetState:function(e,t,n){e=e._reactInternals;var o=_e(),i=Ot(e),a=vt(o,i);a.payload=t,n!=null&&(a.callback=n),t=Mt(e,a,i),t!==null&&(at(t,e,i,o),mo(t,e,i))},enqueueReplaceState:function(e,t,n){e=e._reactInternals;var o=_e(),i=Ot(e),a=vt(o,i);a.tag=1,a.payload=t,n!=null&&(a.callback=n),t=Mt(e,a,i),t!==null&&(at(t,e,i,o),mo(t,e,i))},enqueueForceUpdate:function(e,t){e=e._reactInternals;var n=_e(),o=Ot(e),i=vt(n,o);i.tag=2,t!=null&&(i.callback=t),t=Mt(e,i,o),t!==null&&(at(t,e,o,n),mo(t,e,o))}};function Pl(e,t,n,o,i,a,s){return e=e.stateNode,typeof e.shouldComponentUpdate=="function"?e.shouldComponentUpdate(o,a,s):t.prototype&&t.prototype.isPureReactComponent?!Nr(n,o)||!Nr(i,a):!0}function Jd(e,t,n){var o=!1,i=Ut,a=t.contextType;return typeof a=="object"&&a!==null?a=Ze(a):(i=Oe(t)?qt:Te.current,o=t.contextTypes,a=(o=o!=null)?Pn(e,i):Ut),t=new t(n,a),e.memoizedState=t.state!==null&&t.state!==void 0?t.state:null,t.updater=ei,e.stateNode=t,t._reactInternals=e,o&&(e=e.stateNode,e.__reactInternalMemoizedUnmaskedChildContext=i,e.__reactInternalMemoizedMaskedChildContext=a),t}function Dl(e,t,n,o){e=t.state,typeof t.componentWillReceiveProps=="function"&&t.componentWillReceiveProps(n,o),typeof t.UNSAFE_componentWillReceiveProps=="function"&&t.UNSAFE_componentWillReceiveProps(n,o),t.state!==e&&ei.enqueueReplaceState(t,t.state,null)}function ba(e,t,n,o){var i=e.stateNode;i.props=n,i.state=e.memoizedState,i.refs={},cs(e);var a=t.contextType;typeof a=="object"&&a!==null?i.context=Ze(a):(a=Oe(t)?qt:Te.current,i.context=Pn(e,a)),i.state=e.memoizedState,a=t.getDerivedStateFromProps,typeof a=="function"&&(ya(e,t,a,n),i.state=e.memoizedState),typeof t.getDerivedStateFromProps=="function"||typeof i.getSnapshotBeforeUpdate=="function"||typeof i.UNSAFE_componentWillMount!="function"&&typeof i.componentWillMount!="function"||(t=i.state,typeof i.componentWillMount=="function"&&i.componentWillMount(),typeof i.UNSAFE_componentWillMount=="function"&&i.UNSAFE_componentWillMount(),t!==i.state&&ei.enqueueReplaceState(i,i.state,null),Mo(e,n,i,o),i.state=e.memoizedState),typeof i.componentDidMount=="function"&&(e.flags|=4194308)}function An(e,t){try{var n="",o=t;do n+=_p(o),o=o.return;while(o);var i=n}catch(a){i=`
Error generating stack: `+a.message+`
`+a.stack}return{value:e,source:t,stack:i,digest:null}}function Ii(e,t,n){return{value:e,source:null,stack:n??null,digest:t??null}}function ka(e,t){try{console.error(t.value)}catch(n){setTimeout(function(){throw n})}}var om=typeof WeakMap=="function"?WeakMap:Map;function Zd(e,t,n){n=vt(-1,n),n.tag=3,n.payload={element:null};var o=t.value;return n.callback=function(){Uo||(Uo=!0,Ra=o),ka(e,t)},n}function qd(e,t,n){n=vt(-1,n),n.tag=3;var o=e.type.getDerivedStateFromError;if(typeof o=="function"){var i=t.value;n.payload=function(){return o(i)},n.callback=function(){ka(e,t)}}var a=e.stateNode;return a!==null&&typeof a.componentDidCatch=="function"&&(n.callback=function(){ka(e,t),typeof o!="function"&&(At===null?At=new Set([this]):At.add(this));var s=t.stack;this.componentDidCatch(t.value,{componentStack:s!==null?s:""})}),n}function Il(e,t,n){var o=e.pingCache;if(o===null){o=e.pingCache=new om;var i=new Set;o.set(t,i)}else i=o.get(t),i===void 0&&(i=new Set,o.set(t,i));i.has(n)||(i.add(n),e=vm.bind(null,e,t,n),t.then(e,e))}function Ml(e){do{var t;if((t=e.tag===13)&&(t=e.memoizedState,t=t!==null?t.dehydrated!==null:!0),t)return e;e=e.return}while(e!==null);return null}function Al(e,t,n,o,i){return e.mode&1?(e.flags|=65536,e.lanes=i,e):(e===t?e.flags|=65536:(e.flags|=128,n.flags|=131072,n.flags&=-52805,n.tag===1&&(n.alternate===null?n.tag=17:(t=vt(-1,1),t.tag=2,Mt(n,t,1))),n.lanes|=1),e)}var im=wt.ReactCurrentOwner,Me=!1;function ze(e,t,n,o){t.child=e===null?Ed(t,null,n,o):In(t,e.child,n,o)}function Ol(e,t,n,o,i){n=n.render;var a=t.ref;return _n(t,i),o=ms(e,t,n,o,a,i),n=gs(),e!==null&&!Me?(t.updateQueue=e.updateQueue,t.flags&=-2053,e.lanes&=~i,jt(e,t,i)):(oe&&n&&ns(t),t.flags|=1,ze(e,t,o,i),t.child)}function Bl(e,t,n,o,i){if(e===null){var a=n.type;return typeof a=="function"&&!Ns(a)&&a.defaultProps===void 0&&n.compare===null&&n.defaultProps===void 0?(t.tag=15,t.type=a,eu(e,t,a,o,i)):(e=bo(n.type,null,o,t,t.mode,i),e.ref=t.ref,e.return=t,t.child=e)}if(a=e.child,!(e.lanes&i)){var s=a.memoizedProps;if(n=n.compare,n=n!==null?n:Nr,n(s,o)&&e.ref===t.ref)return jt(e,t,i)}return t.flags|=1,e=Bt(a,o),e.ref=t.ref,e.return=t,t.child=e}function eu(e,t,n,o,i){if(e!==null){var a=e.memoizedProps;if(Nr(a,o)&&e.ref===t.ref)if(Me=!1,t.pendingProps=o=a,(e.lanes&i)!==0)e.flags&131072&&(Me=!0);else return t.lanes=e.lanes,jt(e,t,i)}return ja(e,t,n,o,i)}function tu(e,t,n){var o=t.pendingProps,i=o.children,a=e!==null?e.memoizedState:null;if(o.mode==="hidden")if(!(t.mode&1))t.memoizedState={baseLanes:0,cachePool:null,transitions:null},ee(Nn,Ue),Ue|=n;else{if(!(n&1073741824))return e=a!==null?a.baseLanes|n:n,t.lanes=t.childLanes=1073741824,t.memoizedState={baseLanes:e,cachePool:null,transitions:null},t.updateQueue=null,ee(Nn,Ue),Ue|=e,null;t.memoizedState={baseLanes:0,cachePool:null,transitions:null},o=a!==null?a.baseLanes:n,ee(Nn,Ue),Ue|=o}else a!==null?(o=a.baseLanes|n,t.memoizedState=null):o=n,ee(Nn,Ue),Ue|=o;return ze(e,t,i,n),t.child}function nu(e,t){var n=t.ref;(e===null&&n!==null||e!==null&&e.ref!==n)&&(t.flags|=512,t.flags|=2097152)}function ja(e,t,n,o,i){var a=Oe(n)?qt:Te.current;return a=Pn(t,a),_n(t,i),n=ms(e,t,n,o,a,i),o=gs(),e!==null&&!Me?(t.updateQueue=e.updateQueue,t.flags&=-2053,e.lanes&=~i,jt(e,t,i)):(oe&&o&&ns(t),t.flags|=1,ze(e,t,n,i),t.child)}function Fl(e,t,n,o,i){if(Oe(n)){var a=!0;Ro(t)}else a=!1;if(_n(t,i),t.stateNode===null)xo(e,t),Jd(t,n,o),ba(t,n,o,i),o=!0;else if(e===null){var s=t.stateNode,l=t.memoizedProps;s.props=l;var c=s.context,d=n.contextType;typeof d=="object"&&d!==null?d=Ze(d):(d=Oe(n)?qt:Te.current,d=Pn(t,d));var h=n.getDerivedStateFromProps,f=typeof h=="function"||typeof s.getSnapshotBeforeUpdate=="function";f||typeof s.UNSAFE_componentWillReceiveProps!="function"&&typeof s.componentWillReceiveProps!="function"||(l!==o||c!==d)&&Dl(t,s,o,d),Tt=!1;var g=t.memoizedState;s.state=g,Mo(t,o,s,i),c=t.memoizedState,l!==o||g!==c||Ae.current||Tt?(typeof h=="function"&&(ya(t,n,h,o),c=t.memoizedState),(l=Tt||Pl(t,n,l,o,g,c,d))?(f||typeof s.UNSAFE_componentWillMount!="function"&&typeof s.componentWillMount!="function"||(typeof s.componentWillMount=="function"&&s.componentWillMount(),typeof s.UNSAFE_componentWillMount=="function"&&s.UNSAFE_componentWillMount()),typeof s.componentDidMount=="function"&&(t.flags|=4194308)):(typeof s.componentDidMount=="function"&&(t.flags|=4194308),t.memoizedProps=o,t.memoizedState=c),s.props=o,s.state=c,s.context=d,o=l):(typeof s.componentDidMount=="function"&&(t.flags|=4194308),o=!1)}else{s=t.stateNode,_d(e,t),l=t.memoizedProps,d=t.type===t.elementType?l:nt(t.type,l),s.props=d,f=t.pendingProps,g=s.context,c=n.contextType,typeof c=="object"&&c!==null?c=Ze(c):(c=Oe(n)?qt:Te.current,c=Pn(t,c));var y=n.getDerivedStateFromProps;(h=typeof y=="function"||typeof s.getSnapshotBeforeUpdate=="function")||typeof s.UNSAFE_componentWillReceiveProps!="function"&&typeof s.componentWillReceiveProps!="function"||(l!==f||g!==c)&&Dl(t,s,o,c),Tt=!1,g=t.memoizedState,s.state=g,Mo(t,o,s,i);var v=t.memoizedState;l!==f||g!==v||Ae.current||Tt?(typeof y=="function"&&(ya(t,n,y,o),v=t.memoizedState),(d=Tt||Pl(t,n,d,o,g,v,c)||!1)?(h||typeof s.UNSAFE_componentWillUpdate!="function"&&typeof s.componentWillUpdate!="function"||(typeof s.componentWillUpdate=="function"&&s.componentWillUpdate(o,v,c),typeof s.UNSAFE_componentWillUpdate=="function"&&s.UNSAFE_componentWillUpdate(o,v,c)),typeof s.componentDidUpdate=="function"&&(t.flags|=4),typeof s.getSnapshotBeforeUpdate=="function"&&(t.flags|=1024)):(typeof s.componentDidUpdate!="function"||l===e.memoizedProps&&g===e.memoizedState||(t.flags|=4),typeof s.getSnapshotBeforeUpdate!="function"||l===e.memoizedProps&&g===e.memoizedState||(t.flags|=1024),t.memoizedProps=o,t.memoizedState=v),s.props=o,s.state=v,s.context=c,o=d):(typeof s.componentDidUpdate!="function"||l===e.memoizedProps&&g===e.memoizedState||(t.flags|=4),typeof s.getSnapshotBeforeUpdate!="function"||l===e.memoizedProps&&g===e.memoizedState||(t.flags|=1024),o=!1)}return wa(e,t,n,o,a,i)}function wa(e,t,n,o,i,a){nu(e,t);var s=(t.flags&128)!==0;if(!o&&!s)return i&&Nl(t,n,!1),jt(e,t,a);o=t.stateNode,im.current=t;var l=s&&typeof n.getDerivedStateFromError!="function"?null:o.render();return t.flags|=1,e!==null&&s?(t.child=In(t,e.child,null,a),t.child=In(t,null,l,a)):ze(e,t,l,a),t.memoizedState=o.state,i&&Nl(t,n,!0),t.child}function ru(e){var t=e.stateNode;t.pendingContext?Sl(e,t.pendingContext,t.pendingContext!==t.context):t.context&&Sl(e,t.context,!1),ds(e,t.containerInfo)}function Ul(e,t,n,o,i){return Dn(),os(i),t.flags|=256,ze(e,t,n,o),t.child}var Sa={dehydrated:null,treeContext:null,retryLane:0};function Na(e){return{baseLanes:e,cachePool:null,transitions:null}}function ou(e,t,n){var o=t.pendingProps,i=ie.current,a=!1,s=(t.flags&128)!==0,l;if((l=s)||(l=e!==null&&e.memoizedState===null?!1:(i&2)!==0),l?(a=!0,t.flags&=-129):(e===null||e.memoizedState!==null)&&(i|=1),ee(ie,i&1),e===null)return xa(t),e=t.memoizedState,e!==null&&(e=e.dehydrated,e!==null)?(t.mode&1?e.data==="$!"?t.lanes=8:t.lanes=1073741824:t.lanes=1,null):(s=o.children,e=o.fallback,a?(o=t.mode,a=t.child,s={mode:"hidden",children:s},!(o&1)&&a!==null?(a.childLanes=0,a.pendingProps=s):a=ri(s,o,0,null),e=Zt(e,o,n,null),a.return=t,e.return=t,a.sibling=e,t.child=a,t.child.memoizedState=Na(n),t.memoizedState=Sa,e):vs(t,s));if(i=e.memoizedState,i!==null&&(l=i.dehydrated,l!==null))return am(e,t,s,o,l,i,n);if(a){a=o.fallback,s=t.mode,i=e.child,l=i.sibling;var c={mode:"hidden",children:o.children};return!(s&1)&&t.child!==i?(o=t.child,o.childLanes=0,o.pendingProps=c,t.deletions=null):(o=Bt(i,c),o.subtreeFlags=i.subtreeFlags&14680064),l!==null?a=Bt(l,a):(a=Zt(a,s,n,null),a.flags|=2),a.return=t,o.return=t,o.sibling=a,t.child=o,o=a,a=t.child,s=e.child.memoizedState,s=s===null?Na(n):{baseLanes:s.baseLanes|n,cachePool:null,transitions:s.transitions},a.memoizedState=s,a.childLanes=e.childLanes&~n,t.memoizedState=Sa,o}return a=e.child,e=a.sibling,o=Bt(a,{mode:"visible",children:o.children}),!(t.mode&1)&&(o.lanes=n),o.return=t,o.sibling=null,e!==null&&(n=t.deletions,n===null?(t.deletions=[e],t.flags|=16):n.push(e)),t.child=o,t.memoizedState=null,o}function vs(e,t){return t=ri({mode:"visible",children:t},e.mode,0,null),t.return=e,e.child=t}function eo(e,t,n,o){return o!==null&&os(o),In(t,e.child,null,n),e=vs(t,t.pendingProps.children),e.flags|=2,t.memoizedState=null,e}function am(e,t,n,o,i,a,s){if(n)return t.flags&256?(t.flags&=-257,o=Ii(Error(L(422))),eo(e,t,s,o)):t.memoizedState!==null?(t.child=e.child,t.flags|=128,null):(a=o.fallback,i=t.mode,o=ri({mode:"visible",children:o.children},i,0,null),a=Zt(a,i,s,null),a.flags|=2,o.return=t,a.return=t,o.sibling=a,t.child=o,t.mode&1&&In(t,e.child,null,s),t.child.memoizedState=Na(s),t.memoizedState=Sa,a);if(!(t.mode&1))return eo(e,t,s,null);if(i.data==="$!"){if(o=i.nextSibling&&i.nextSibling.dataset,o)var l=o.dgst;return o=l,a=Error(L(419)),o=Ii(a,o,void 0),eo(e,t,s,o)}if(l=(s&e.childLanes)!==0,Me||l){if(o=ve,o!==null){switch(s&-s){case 4:i=2;break;case 16:i=8;break;case 64:case 128:case 256:case 512:case 1024:case 2048:case 4096:case 8192:case 16384:case 32768:case 65536:case 131072:case 262144:case 524288:case 1048576:case 2097152:case 4194304:case 8388608:case 16777216:case 33554432:case 67108864:i=32;break;case 536870912:i=268435456;break;default:i=0}i=i&(o.suspendedLanes|s)?0:i,i!==0&&i!==a.retryLane&&(a.retryLane=i,kt(e,i),at(o,e,i,-1))}return Ss(),o=Ii(Error(L(421))),eo(e,t,s,o)}return i.data==="$?"?(t.flags|=128,t.child=e.child,t=ym.bind(null,e),i._reactRetry=t,null):(e=a.treeContext,We=It(i.nextSibling),$e=t,oe=!0,ot=null,e!==null&&(Xe[Qe++]=ht,Xe[Qe++]=xt,Xe[Qe++]=en,ht=e.id,xt=e.overflow,en=t),t=vs(t,o.children),t.flags|=4096,t)}function Wl(e,t,n){e.lanes|=t;var o=e.alternate;o!==null&&(o.lanes|=t),va(e.return,t,n)}function Mi(e,t,n,o,i){var a=e.memoizedState;a===null?e.memoizedState={isBackwards:t,rendering:null,renderingStartTime:0,last:o,tail:n,tailMode:i}:(a.isBackwards=t,a.rendering=null,a.renderingStartTime=0,a.last=o,a.tail=n,a.tailMode=i)}function iu(e,t,n){var o=t.pendingProps,i=o.revealOrder,a=o.tail;if(ze(e,t,o.children,n),o=ie.current,o&2)o=o&1|2,t.flags|=128;else{if(e!==null&&e.flags&128)e:for(e=t.child;e!==null;){if(e.tag===13)e.memoizedState!==null&&Wl(e,n,t);else if(e.tag===19)Wl(e,n,t);else if(e.child!==null){e.child.return=e,e=e.child;continue}if(e===t)break e;for(;e.sibling===null;){if(e.return===null||e.return===t)break e;e=e.return}e.sibling.return=e.return,e=e.sibling}o&=1}if(ee(ie,o),!(t.mode&1))t.memoizedState=null;else switch(i){case"forwards":for(n=t.child,i=null;n!==null;)e=n.alternate,e!==null&&Ao(e)===null&&(i=n),n=n.sibling;n=i,n===null?(i=t.child,t.child=null):(i=n.sibling,n.sibling=null),Mi(t,!1,i,n,a);break;case"backwards":for(n=null,i=t.child,t.child=null;i!==null;){if(e=i.alternate,e!==null&&Ao(e)===null){t.child=i;break}e=i.sibling,i.sibling=n,n=i,i=e}Mi(t,!0,n,null,a);break;case"together":Mi(t,!1,null,null,void 0);break;default:t.memoizedState=null}return t.child}function xo(e,t){!(t.mode&1)&&e!==null&&(e.alternate=null,t.alternate=null,t.flags|=2)}function jt(e,t,n){if(e!==null&&(t.dependencies=e.dependencies),nn|=t.lanes,!(n&t.childLanes))return null;if(e!==null&&t.child!==e.child)throw Error(L(153));if(t.child!==null){for(e=t.child,n=Bt(e,e.pendingProps),t.child=n,n.return=t;e.sibling!==null;)e=e.sibling,n=n.sibling=Bt(e,e.pendingProps),n.return=t;n.sibling=null}return t.child}function sm(e,t,n){switch(t.tag){case 3:ru(t),Dn();break;case 5:Rd(t);break;case 1:Oe(t.type)&&Ro(t);break;case 4:ds(t,t.stateNode.containerInfo);break;case 10:var o=t.type._context,i=t.memoizedProps.value;ee(Do,o._currentValue),o._currentValue=i;break;case 13:if(o=t.memoizedState,o!==null)return o.dehydrated!==null?(ee(ie,ie.current&1),t.flags|=128,null):n&t.child.childLanes?ou(e,t,n):(ee(ie,ie.current&1),e=jt(e,t,n),e!==null?e.sibling:null);ee(ie,ie.current&1);break;case 19:if(o=(n&t.childLanes)!==0,e.flags&128){if(o)return iu(e,t,n);t.flags|=128}if(i=t.memoizedState,i!==null&&(i.rendering=null,i.tail=null,i.lastEffect=null),ee(ie,ie.current),o)break;return null;case 22:case 23:return t.lanes=0,tu(e,t,n)}return jt(e,t,n)}var au,Ca,su,lu;au=function(e,t){for(var n=t.child;n!==null;){if(n.tag===5||n.tag===6)e.appendChild(n.stateNode);else if(n.tag!==4&&n.child!==null){n.child.return=n,n=n.child;continue}if(n===t)break;for(;n.sibling===null;){if(n.return===null||n.return===t)return;n=n.return}n.sibling.return=n.return,n=n.sibling}};Ca=function(){};su=function(e,t,n,o){var i=e.memoizedProps;if(i!==o){e=t.stateNode,Yt(pt.current);var a=null;switch(n){case"input":i=Xi(e,i),o=Xi(e,o),a=[];break;case"select":i=se({},i,{value:void 0}),o=se({},o,{value:void 0}),a=[];break;case"textarea":i=Ji(e,i),o=Ji(e,o),a=[];break;default:typeof i.onClick!="function"&&typeof o.onClick=="function"&&(e.onclick=zo)}qi(n,o);var s;n=null;for(d in i)if(!o.hasOwnProperty(d)&&i.hasOwnProperty(d)&&i[d]!=null)if(d==="style"){var l=i[d];for(s in l)l.hasOwnProperty(s)&&(n||(n={}),n[s]="")}else d!=="dangerouslySetInnerHTML"&&d!=="children"&&d!=="suppressContentEditableWarning"&&d!=="suppressHydrationWarning"&&d!=="autoFocus"&&(vr.hasOwnProperty(d)?a||(a=[]):(a=a||[]).push(d,null));for(d in o){var c=o[d];if(l=i!=null?i[d]:void 0,o.hasOwnProperty(d)&&c!==l&&(c!=null||l!=null))if(d==="style")if(l){for(s in l)!l.hasOwnProperty(s)||c&&c.hasOwnProperty(s)||(n||(n={}),n[s]="");for(s in c)c.hasOwnProperty(s)&&l[s]!==c[s]&&(n||(n={}),n[s]=c[s])}else n||(a||(a=[]),a.push(d,n)),n=c;else d==="dangerouslySetInnerHTML"?(c=c?c.__html:void 0,l=l?l.__html:void 0,c!=null&&l!==c&&(a=a||[]).push(d,c)):d==="children"?typeof c!="string"&&typeof c!="number"||(a=a||[]).push(d,""+c):d!=="suppressContentEditableWarning"&&d!=="suppressHydrationWarning"&&(vr.hasOwnProperty(d)?(c!=null&&d==="onScroll"&&ne("scroll",e),a||l===c||(a=[])):(a=a||[]).push(d,c))}n&&(a=a||[]).push("style",n);var d=a;(t.updateQueue=d)&&(t.flags|=4)}};lu=function(e,t,n,o){n!==o&&(t.flags|=4)};function er(e,t){if(!oe)switch(e.tailMode){case"hidden":t=e.tail;for(var n=null;t!==null;)t.alternate!==null&&(n=t),t=t.sibling;n===null?e.tail=null:n.sibling=null;break;case"collapsed":n=e.tail;for(var o=null;n!==null;)n.alternate!==null&&(o=n),n=n.sibling;o===null?t||e.tail===null?e.tail=null:e.tail.sibling=null:o.sibling=null}}function Ne(e){var t=e.alternate!==null&&e.alternate.child===e.child,n=0,o=0;if(t)for(var i=e.child;i!==null;)n|=i.lanes|i.childLanes,o|=i.subtreeFlags&14680064,o|=i.flags&14680064,i.return=e,i=i.sibling;else for(i=e.child;i!==null;)n|=i.lanes|i.childLanes,o|=i.subtreeFlags,o|=i.flags,i.return=e,i=i.sibling;return e.subtreeFlags|=o,e.childLanes=n,t}function lm(e,t,n){var o=t.pendingProps;switch(rs(t),t.tag){case 2:case 16:case 15:case 0:case 11:case 7:case 8:case 12:case 9:case 14:return Ne(t),null;case 1:return Oe(t.type)&&_o(),Ne(t),null;case 3:return o=t.stateNode,Mn(),re(Ae),re(Te),ps(),o.pendingContext&&(o.context=o.pendingContext,o.pendingContext=null),(e===null||e.child===null)&&(Zr(t)?t.flags|=4:e===null||e.memoizedState.isDehydrated&&!(t.flags&256)||(t.flags|=1024,ot!==null&&(Da(ot),ot=null))),Ca(e,t),Ne(t),null;case 5:us(t);var i=Yt(_r.current);if(n=t.type,e!==null&&t.stateNode!=null)su(e,t,n,o,i),e.ref!==t.ref&&(t.flags|=512,t.flags|=2097152);else{if(!o){if(t.stateNode===null)throw Error(L(166));return Ne(t),null}if(e=Yt(pt.current),Zr(t)){o=t.stateNode,n=t.type;var a=t.memoizedProps;switch(o[dt]=t,o[Er]=a,e=(t.mode&1)!==0,n){case"dialog":ne("cancel",o),ne("close",o);break;case"iframe":case"object":case"embed":ne("load",o);break;case"video":case"audio":for(i=0;i<sr.length;i++)ne(sr[i],o);break;case"source":ne("error",o);break;case"img":case"image":case"link":ne("error",o),ne("load",o);break;case"details":ne("toggle",o);break;case"input":Js(o,a),ne("invalid",o);break;case"select":o._wrapperState={wasMultiple:!!a.multiple},ne("invalid",o);break;case"textarea":qs(o,a),ne("invalid",o)}qi(n,a),i=null;for(var s in a)if(a.hasOwnProperty(s)){var l=a[s];s==="children"?typeof l=="string"?o.textContent!==l&&(a.suppressHydrationWarning!==!0&&Jr(o.textContent,l,e),i=["children",l]):typeof l=="number"&&o.textContent!==""+l&&(a.suppressHydrationWarning!==!0&&Jr(o.textContent,l,e),i=["children",""+l]):vr.hasOwnProperty(s)&&l!=null&&s==="onScroll"&&ne("scroll",o)}switch(n){case"input":$r(o),Zs(o,a,!0);break;case"textarea":$r(o),el(o);break;case"select":case"option":break;default:typeof a.onClick=="function"&&(o.onclick=zo)}o=i,t.updateQueue=o,o!==null&&(t.flags|=4)}else{s=i.nodeType===9?i:i.ownerDocument,e==="http://www.w3.org/1999/xhtml"&&(e=Mc(n)),e==="http://www.w3.org/1999/xhtml"?n==="script"?(e=s.createElement("div"),e.innerHTML="<script><\/script>",e=e.removeChild(e.firstChild)):typeof o.is=="string"?e=s.createElement(n,{is:o.is}):(e=s.createElement(n),n==="select"&&(s=e,o.multiple?s.multiple=!0:o.size&&(s.size=o.size))):e=s.createElementNS(e,n),e[dt]=t,e[Er]=o,au(e,t,!1,!1),t.stateNode=e;e:{switch(s=ea(n,o),n){case"dialog":ne("cancel",e),ne("close",e),i=o;break;case"iframe":case"object":case"embed":ne("load",e),i=o;break;case"video":case"audio":for(i=0;i<sr.length;i++)ne(sr[i],e);i=o;break;case"source":ne("error",e),i=o;break;case"img":case"image":case"link":ne("error",e),ne("load",e),i=o;break;case"details":ne("toggle",e),i=o;break;case"input":Js(e,o),i=Xi(e,o),ne("invalid",e);break;case"option":i=o;break;case"select":e._wrapperState={wasMultiple:!!o.multiple},i=se({},o,{value:void 0}),ne("invalid",e);break;case"textarea":qs(e,o),i=Ji(e,o),ne("invalid",e);break;default:i=o}qi(n,i),l=i;for(a in l)if(l.hasOwnProperty(a)){var c=l[a];a==="style"?Bc(e,c):a==="dangerouslySetInnerHTML"?(c=c?c.__html:void 0,c!=null&&Ac(e,c)):a==="children"?typeof c=="string"?(n!=="textarea"||c!=="")&&yr(e,c):typeof c=="number"&&yr(e,""+c):a!=="suppressContentEditableWarning"&&a!=="suppressHydrationWarning"&&a!=="autoFocus"&&(vr.hasOwnProperty(a)?c!=null&&a==="onScroll"&&ne("scroll",e):c!=null&&Wa(e,a,c,s))}switch(n){case"input":$r(e),Zs(e,o,!1);break;case"textarea":$r(e),el(e);break;case"option":o.value!=null&&e.setAttribute("value",""+Ft(o.value));break;case"select":e.multiple=!!o.multiple,a=o.value,a!=null?Cn(e,!!o.multiple,a,!1):o.defaultValue!=null&&Cn(e,!!o.multiple,o.defaultValue,!0);break;default:typeof i.onClick=="function"&&(e.onclick=zo)}switch(n){case"button":case"input":case"select":case"textarea":o=!!o.autoFocus;break e;case"img":o=!0;break e;default:o=!1}}o&&(t.flags|=4)}t.ref!==null&&(t.flags|=512,t.flags|=2097152)}return Ne(t),null;case 6:if(e&&t.stateNode!=null)lu(e,t,e.memoizedProps,o);else{if(typeof o!="string"&&t.stateNode===null)throw Error(L(166));if(n=Yt(_r.current),Yt(pt.current),Zr(t)){if(o=t.stateNode,n=t.memoizedProps,o[dt]=t,(a=o.nodeValue!==n)&&(e=$e,e!==null))switch(e.tag){case 3:Jr(o.nodeValue,n,(e.mode&1)!==0);break;case 5:e.memoizedProps.suppressHydrationWarning!==!0&&Jr(o.nodeValue,n,(e.mode&1)!==0)}a&&(t.flags|=4)}else o=(n.nodeType===9?n:n.ownerDocument).createTextNode(o),o[dt]=t,t.stateNode=o}return Ne(t),null;case 13:if(re(ie),o=t.memoizedState,e===null||e.memoizedState!==null&&e.memoizedState.dehydrated!==null){if(oe&&We!==null&&t.mode&1&&!(t.flags&128))Cd(),Dn(),t.flags|=98560,a=!1;else if(a=Zr(t),o!==null&&o.dehydrated!==null){if(e===null){if(!a)throw Error(L(318));if(a=t.memoizedState,a=a!==null?a.dehydrated:null,!a)throw Error(L(317));a[dt]=t}else Dn(),!(t.flags&128)&&(t.memoizedState=null),t.flags|=4;Ne(t),a=!1}else ot!==null&&(Da(ot),ot=null),a=!0;if(!a)return t.flags&65536?t:null}return t.flags&128?(t.lanes=n,t):(o=o!==null,o!==(e!==null&&e.memoizedState!==null)&&o&&(t.child.flags|=8192,t.mode&1&&(e===null||ie.current&1?ge===0&&(ge=3):Ss())),t.updateQueue!==null&&(t.flags|=4),Ne(t),null);case 4:return Mn(),Ca(e,t),e===null&&Cr(t.stateNode.containerInfo),Ne(t),null;case 10:return ss(t.type._context),Ne(t),null;case 17:return Oe(t.type)&&_o(),Ne(t),null;case 19:if(re(ie),a=t.memoizedState,a===null)return Ne(t),null;if(o=(t.flags&128)!==0,s=a.rendering,s===null)if(o)er(a,!1);else{if(ge!==0||e!==null&&e.flags&128)for(e=t.child;e!==null;){if(s=Ao(e),s!==null){for(t.flags|=128,er(a,!1),o=s.updateQueue,o!==null&&(t.updateQueue=o,t.flags|=4),t.subtreeFlags=0,o=n,n=t.child;n!==null;)a=n,e=o,a.flags&=14680066,s=a.alternate,s===null?(a.childLanes=0,a.lanes=e,a.child=null,a.subtreeFlags=0,a.memoizedProps=null,a.memoizedState=null,a.updateQueue=null,a.dependencies=null,a.stateNode=null):(a.childLanes=s.childLanes,a.lanes=s.lanes,a.child=s.child,a.subtreeFlags=0,a.deletions=null,a.memoizedProps=s.memoizedProps,a.memoizedState=s.memoizedState,a.updateQueue=s.updateQueue,a.type=s.type,e=s.dependencies,a.dependencies=e===null?null:{lanes:e.lanes,firstContext:e.firstContext}),n=n.sibling;return ee(ie,ie.current&1|2),t.child}e=e.sibling}a.tail!==null&&de()>On&&(t.flags|=128,o=!0,er(a,!1),t.lanes=4194304)}else{if(!o)if(e=Ao(s),e!==null){if(t.flags|=128,o=!0,n=e.updateQueue,n!==null&&(t.updateQueue=n,t.flags|=4),er(a,!0),a.tail===null&&a.tailMode==="hidden"&&!s.alternate&&!oe)return Ne(t),null}else 2*de()-a.renderingStartTime>On&&n!==1073741824&&(t.flags|=128,o=!0,er(a,!1),t.lanes=4194304);a.isBackwards?(s.sibling=t.child,t.child=s):(n=a.last,n!==null?n.sibling=s:t.child=s,a.last=s)}return a.tail!==null?(t=a.tail,a.rendering=t,a.tail=t.sibling,a.renderingStartTime=de(),t.sibling=null,n=ie.current,ee(ie,o?n&1|2:n&1),t):(Ne(t),null);case 22:case 23:return ws(),o=t.memoizedState!==null,e!==null&&e.memoizedState!==null!==o&&(t.flags|=8192),o&&t.mode&1?Ue&1073741824&&(Ne(t),t.subtreeFlags&6&&(t.flags|=8192)):Ne(t),null;case 24:return null;case 25:return null}throw Error(L(156,t.tag))}function cm(e,t){switch(rs(t),t.tag){case 1:return Oe(t.type)&&_o(),e=t.flags,e&65536?(t.flags=e&-65537|128,t):null;case 3:return Mn(),re(Ae),re(Te),ps(),e=t.flags,e&65536&&!(e&128)?(t.flags=e&-65537|128,t):null;case 5:return us(t),null;case 13:if(re(ie),e=t.memoizedState,e!==null&&e.dehydrated!==null){if(t.alternate===null)throw Error(L(340));Dn()}return e=t.flags,e&65536?(t.flags=e&-65537|128,t):null;case 19:return re(ie),null;case 4:return Mn(),null;case 10:return ss(t.type._context),null;case 22:case 23:return ws(),null;case 24:return null;default:return null}}var to=!1,Ce=!1,dm=typeof WeakSet=="function"?WeakSet:Set,A=null;function Sn(e,t){var n=e.ref;if(n!==null)if(typeof n=="function")try{n(null)}catch(o){ce(e,t,o)}else n.current=null}function Ta(e,t,n){try{n()}catch(o){ce(e,t,o)}}var $l=!1;function um(e,t){if(da=Co,e=fd(),ts(e)){if("selectionStart"in e)var n={start:e.selectionStart,end:e.selectionEnd};else e:{n=(n=e.ownerDocument)&&n.defaultView||window;var o=n.getSelection&&n.getSelection();if(o&&o.rangeCount!==0){n=o.anchorNode;var i=o.anchorOffset,a=o.focusNode;o=o.focusOffset;try{n.nodeType,a.nodeType}catch{n=null;break e}var s=0,l=-1,c=-1,d=0,h=0,f=e,g=null;t:for(;;){for(var y;f!==n||i!==0&&f.nodeType!==3||(l=s+i),f!==a||o!==0&&f.nodeType!==3||(c=s+o),f.nodeType===3&&(s+=f.nodeValue.length),(y=f.firstChild)!==null;)g=f,f=y;for(;;){if(f===e)break t;if(g===n&&++d===i&&(l=s),g===a&&++h===o&&(c=s),(y=f.nextSibling)!==null)break;f=g,g=f.parentNode}f=y}n=l===-1||c===-1?null:{start:l,end:c}}else n=null}n=n||{start:0,end:0}}else n=null;for(ua={focusedElem:e,selectionRange:n},Co=!1,A=t;A!==null;)if(t=A,e=t.child,(t.subtreeFlags&1028)!==0&&e!==null)e.return=t,A=e;else for(;A!==null;){t=A;try{var v=t.alternate;if(t.flags&1024)switch(t.tag){case 0:case 11:case 15:break;case 1:if(v!==null){var b=v.memoizedProps,z=v.memoizedState,p=t.stateNode,u=p.getSnapshotBeforeUpdate(t.elementType===t.type?b:nt(t.type,b),z);p.__reactInternalSnapshotBeforeUpdate=u}break;case 3:var m=t.stateNode.containerInfo;m.nodeType===1?m.textContent="":m.nodeType===9&&m.documentElement&&m.removeChild(m.documentElement);break;case 5:case 6:case 4:case 17:break;default:throw Error(L(163))}}catch(j){ce(t,t.return,j)}if(e=t.sibling,e!==null){e.return=t.return,A=e;break}A=t.return}return v=$l,$l=!1,v}function gr(e,t,n){var o=t.updateQueue;if(o=o!==null?o.lastEffect:null,o!==null){var i=o=o.next;do{if((i.tag&e)===e){var a=i.destroy;i.destroy=void 0,a!==void 0&&Ta(t,n,a)}i=i.next}while(i!==o)}}function ti(e,t){if(t=t.updateQueue,t=t!==null?t.lastEffect:null,t!==null){var n=t=t.next;do{if((n.tag&e)===e){var o=n.create;n.destroy=o()}n=n.next}while(n!==t)}}function Ea(e){var t=e.ref;if(t!==null){var n=e.stateNode;switch(e.tag){case 5:e=n;break;default:e=n}typeof t=="function"?t(e):t.current=e}}function cu(e){var t=e.alternate;t!==null&&(e.alternate=null,cu(t)),e.child=null,e.deletions=null,e.sibling=null,e.tag===5&&(t=e.stateNode,t!==null&&(delete t[dt],delete t[Er],delete t[ma],delete t[Gf],delete t[Xf])),e.stateNode=null,e.return=null,e.dependencies=null,e.memoizedProps=null,e.memoizedState=null,e.pendingProps=null,e.stateNode=null,e.updateQueue=null}function du(e){return e.tag===5||e.tag===3||e.tag===4}function Hl(e){e:for(;;){for(;e.sibling===null;){if(e.return===null||du(e.return))return null;e=e.return}for(e.sibling.return=e.return,e=e.sibling;e.tag!==5&&e.tag!==6&&e.tag!==18;){if(e.flags&2||e.child===null||e.tag===4)continue e;e.child.return=e,e=e.child}if(!(e.flags&2))return e.stateNode}}function za(e,t,n){var o=e.tag;if(o===5||o===6)e=e.stateNode,t?n.nodeType===8?n.parentNode.insertBefore(e,t):n.insertBefore(e,t):(n.nodeType===8?(t=n.parentNode,t.insertBefore(e,n)):(t=n,t.appendChild(e)),n=n._reactRootContainer,n!=null||t.onclick!==null||(t.onclick=zo));else if(o!==4&&(e=e.child,e!==null))for(za(e,t,n),e=e.sibling;e!==null;)za(e,t,n),e=e.sibling}function _a(e,t,n){var o=e.tag;if(o===5||o===6)e=e.stateNode,t?n.insertBefore(e,t):n.appendChild(e);else if(o!==4&&(e=e.child,e!==null))for(_a(e,t,n),e=e.sibling;e!==null;)_a(e,t,n),e=e.sibling}var be=null,rt=!1;function Nt(e,t,n){for(n=n.child;n!==null;)uu(e,t,n),n=n.sibling}function uu(e,t,n){if(ut&&typeof ut.onCommitFiberUnmount=="function")try{ut.onCommitFiberUnmount(Go,n)}catch{}switch(n.tag){case 5:Ce||Sn(n,t);case 6:var o=be,i=rt;be=null,Nt(e,t,n),be=o,rt=i,be!==null&&(rt?(e=be,n=n.stateNode,e.nodeType===8?e.parentNode.removeChild(n):e.removeChild(n)):be.removeChild(n.stateNode));break;case 18:be!==null&&(rt?(e=be,n=n.stateNode,e.nodeType===8?zi(e.parentNode,n):e.nodeType===1&&zi(e,n),wr(e)):zi(be,n.stateNode));break;case 4:o=be,i=rt,be=n.stateNode.containerInfo,rt=!0,Nt(e,t,n),be=o,rt=i;break;case 0:case 11:case 14:case 15:if(!Ce&&(o=n.updateQueue,o!==null&&(o=o.lastEffect,o!==null))){i=o=o.next;do{var a=i,s=a.destroy;a=a.tag,s!==void 0&&(a&2||a&4)&&Ta(n,t,s),i=i.next}while(i!==o)}Nt(e,t,n);break;case 1:if(!Ce&&(Sn(n,t),o=n.stateNode,typeof o.componentWillUnmount=="function"))try{o.props=n.memoizedProps,o.state=n.memoizedState,o.componentWillUnmount()}catch(l){ce(n,t,l)}Nt(e,t,n);break;case 21:Nt(e,t,n);break;case 22:n.mode&1?(Ce=(o=Ce)||n.memoizedState!==null,Nt(e,t,n),Ce=o):Nt(e,t,n);break;default:Nt(e,t,n)}}function Kl(e){var t=e.updateQueue;if(t!==null){e.updateQueue=null;var n=e.stateNode;n===null&&(n=e.stateNode=new dm),t.forEach(function(o){var i=bm.bind(null,e,o);n.has(o)||(n.add(o),o.then(i,i))})}}function tt(e,t){var n=t.deletions;if(n!==null)for(var o=0;o<n.length;o++){var i=n[o];try{var a=e,s=t,l=s;e:for(;l!==null;){switch(l.tag){case 5:be=l.stateNode,rt=!1;break e;case 3:be=l.stateNode.containerInfo,rt=!0;break e;case 4:be=l.stateNode.containerInfo,rt=!0;break e}l=l.return}if(be===null)throw Error(L(160));uu(a,s,i),be=null,rt=!1;var c=i.alternate;c!==null&&(c.return=null),i.return=null}catch(d){ce(i,t,d)}}if(t.subtreeFlags&12854)for(t=t.child;t!==null;)pu(t,e),t=t.sibling}function pu(e,t){var n=e.alternate,o=e.flags;switch(e.tag){case 0:case 11:case 14:case 15:if(tt(t,e),lt(e),o&4){try{gr(3,e,e.return),ti(3,e)}catch(b){ce(e,e.return,b)}try{gr(5,e,e.return)}catch(b){ce(e,e.return,b)}}break;case 1:tt(t,e),lt(e),o&512&&n!==null&&Sn(n,n.return);break;case 5:if(tt(t,e),lt(e),o&512&&n!==null&&Sn(n,n.return),e.flags&32){var i=e.stateNode;try{yr(i,"")}catch(b){ce(e,e.return,b)}}if(o&4&&(i=e.stateNode,i!=null)){var a=e.memoizedProps,s=n!==null?n.memoizedProps:a,l=e.type,c=e.updateQueue;if(e.updateQueue=null,c!==null)try{l==="input"&&a.type==="radio"&&a.name!=null&&Dc(i,a),ea(l,s);var d=ea(l,a);for(s=0;s<c.length;s+=2){var h=c[s],f=c[s+1];h==="style"?Bc(i,f):h==="dangerouslySetInnerHTML"?Ac(i,f):h==="children"?yr(i,f):Wa(i,h,f,d)}switch(l){case"input":Qi(i,a);break;case"textarea":Ic(i,a);break;case"select":var g=i._wrapperState.wasMultiple;i._wrapperState.wasMultiple=!!a.multiple;var y=a.value;y!=null?Cn(i,!!a.multiple,y,!1):g!==!!a.multiple&&(a.defaultValue!=null?Cn(i,!!a.multiple,a.defaultValue,!0):Cn(i,!!a.multiple,a.multiple?[]:"",!1))}i[Er]=a}catch(b){ce(e,e.return,b)}}break;case 6:if(tt(t,e),lt(e),o&4){if(e.stateNode===null)throw Error(L(162));i=e.stateNode,a=e.memoizedProps;try{i.nodeValue=a}catch(b){ce(e,e.return,b)}}break;case 3:if(tt(t,e),lt(e),o&4&&n!==null&&n.memoizedState.isDehydrated)try{wr(t.containerInfo)}catch(b){ce(e,e.return,b)}break;case 4:tt(t,e),lt(e);break;case 13:tt(t,e),lt(e),i=e.child,i.flags&8192&&(a=i.memoizedState!==null,i.stateNode.isHidden=a,!a||i.alternate!==null&&i.alternate.memoizedState!==null||(ks=de())),o&4&&Kl(e);break;case 22:if(h=n!==null&&n.memoizedState!==null,e.mode&1?(Ce=(d=Ce)||h,tt(t,e),Ce=d):tt(t,e),lt(e),o&8192){if(d=e.memoizedState!==null,(e.stateNode.isHidden=d)&&!h&&e.mode&1)for(A=e,h=e.child;h!==null;){for(f=A=h;A!==null;){switch(g=A,y=g.child,g.tag){case 0:case 11:case 14:case 15:gr(4,g,g.return);break;case 1:Sn(g,g.return);var v=g.stateNode;if(typeof v.componentWillUnmount=="function"){o=g,n=g.return;try{t=o,v.props=t.memoizedProps,v.state=t.memoizedState,v.componentWillUnmount()}catch(b){ce(o,n,b)}}break;case 5:Sn(g,g.return);break;case 22:if(g.memoizedState!==null){Gl(f);continue}}y!==null?(y.return=g,A=y):Gl(f)}h=h.sibling}e:for(h=null,f=e;;){if(f.tag===5){if(h===null){h=f;try{i=f.stateNode,d?(a=i.style,typeof a.setProperty=="function"?a.setProperty("display","none","important"):a.display="none"):(l=f.stateNode,c=f.memoizedProps.style,s=c!=null&&c.hasOwnProperty("display")?c.display:null,l.style.display=Oc("display",s))}catch(b){ce(e,e.return,b)}}}else if(f.tag===6){if(h===null)try{f.stateNode.nodeValue=d?"":f.memoizedProps}catch(b){ce(e,e.return,b)}}else if((f.tag!==22&&f.tag!==23||f.memoizedState===null||f===e)&&f.child!==null){f.child.return=f,f=f.child;continue}if(f===e)break e;for(;f.sibling===null;){if(f.return===null||f.return===e)break e;h===f&&(h=null),f=f.return}h===f&&(h=null),f.sibling.return=f.return,f=f.sibling}}break;case 19:tt(t,e),lt(e),o&4&&Kl(e);break;case 21:break;default:tt(t,e),lt(e)}}function lt(e){var t=e.flags;if(t&2){try{e:{for(var n=e.return;n!==null;){if(du(n)){var o=n;break e}n=n.return}throw Error(L(160))}switch(o.tag){case 5:var i=o.stateNode;o.flags&32&&(yr(i,""),o.flags&=-33);var a=Hl(e);_a(e,a,i);break;case 3:case 4:var s=o.stateNode.containerInfo,l=Hl(e);za(e,l,s);break;default:throw Error(L(161))}}catch(c){ce(e,e.return,c)}e.flags&=-3}t&4096&&(e.flags&=-4097)}function pm(e,t,n){A=e,fu(e)}function fu(e,t,n){for(var o=(e.mode&1)!==0;A!==null;){var i=A,a=i.child;if(i.tag===22&&o){var s=i.memoizedState!==null||to;if(!s){var l=i.alternate,c=l!==null&&l.memoizedState!==null||Ce;l=to;var d=Ce;if(to=s,(Ce=c)&&!d)for(A=i;A!==null;)s=A,c=s.child,s.tag===22&&s.memoizedState!==null?Xl(i):c!==null?(c.return=s,A=c):Xl(i);for(;a!==null;)A=a,fu(a),a=a.sibling;A=i,to=l,Ce=d}Vl(e)}else i.subtreeFlags&8772&&a!==null?(a.return=i,A=a):Vl(e)}}function Vl(e){for(;A!==null;){var t=A;if(t.flags&8772){var n=t.alternate;try{if(t.flags&8772)switch(t.tag){case 0:case 11:case 15:Ce||ti(5,t);break;case 1:var o=t.stateNode;if(t.flags&4&&!Ce)if(n===null)o.componentDidMount();else{var i=t.elementType===t.type?n.memoizedProps:nt(t.type,n.memoizedProps);o.componentDidUpdate(i,n.memoizedState,o.__reactInternalSnapshotBeforeUpdate)}var a=t.updateQueue;a!==null&&_l(t,a,o);break;case 3:var s=t.updateQueue;if(s!==null){if(n=null,t.child!==null)switch(t.child.tag){case 5:n=t.child.stateNode;break;case 1:n=t.child.stateNode}_l(t,s,n)}break;case 5:var l=t.stateNode;if(n===null&&t.flags&4){n=l;var c=t.memoizedProps;switch(t.type){case"button":case"input":case"select":case"textarea":c.autoFocus&&n.focus();break;case"img":c.src&&(n.src=c.src)}}break;case 6:break;case 4:break;case 12:break;case 13:if(t.memoizedState===null){var d=t.alternate;if(d!==null){var h=d.memoizedState;if(h!==null){var f=h.dehydrated;f!==null&&wr(f)}}}break;case 19:case 17:case 21:case 22:case 23:case 25:break;default:throw Error(L(163))}Ce||t.flags&512&&Ea(t)}catch(g){ce(t,t.return,g)}}if(t===e){A=null;break}if(n=t.sibling,n!==null){n.return=t.return,A=n;break}A=t.return}}function Gl(e){for(;A!==null;){var t=A;if(t===e){A=null;break}var n=t.sibling;if(n!==null){n.return=t.return,A=n;break}A=t.return}}function Xl(e){for(;A!==null;){var t=A;try{switch(t.tag){case 0:case 11:case 15:var n=t.return;try{ti(4,t)}catch(c){ce(t,n,c)}break;case 1:var o=t.stateNode;if(typeof o.componentDidMount=="function"){var i=t.return;try{o.componentDidMount()}catch(c){ce(t,i,c)}}var a=t.return;try{Ea(t)}catch(c){ce(t,a,c)}break;case 5:var s=t.return;try{Ea(t)}catch(c){ce(t,s,c)}}}catch(c){ce(t,t.return,c)}if(t===e){A=null;break}var l=t.sibling;if(l!==null){l.return=t.return,A=l;break}A=t.return}}var fm=Math.ceil,Fo=wt.ReactCurrentDispatcher,ys=wt.ReactCurrentOwner,Je=wt.ReactCurrentBatchConfig,X=0,ve=null,ue=null,ke=0,Ue=0,Nn=$t(0),ge=0,Dr=null,nn=0,ni=0,bs=0,hr=null,Ie=null,ks=0,On=1/0,mt=null,Uo=!1,Ra=null,At=null,no=!1,Rt=null,Wo=0,xr=0,La=null,vo=-1,yo=0;function _e(){return X&6?de():vo!==-1?vo:vo=de()}function Ot(e){return e.mode&1?X&2&&ke!==0?ke&-ke:Yf.transition!==null?(yo===0&&(yo=Jc()),yo):(e=Z,e!==0||(e=window.event,e=e===void 0?16:od(e.type)),e):1}function at(e,t,n,o){if(50<xr)throw xr=0,La=null,Error(L(185));Mr(e,n,o),(!(X&2)||e!==ve)&&(e===ve&&(!(X&2)&&(ni|=n),ge===4&&zt(e,ke)),Be(e,o),n===1&&X===0&&!(t.mode&1)&&(On=de()+500,Zo&&Ht()))}function Be(e,t){var n=e.callbackNode;Qp(e,t);var o=No(e,e===ve?ke:0);if(o===0)n!==null&&rl(n),e.callbackNode=null,e.callbackPriority=0;else if(t=o&-o,e.callbackPriority!==t){if(n!=null&&rl(n),t===1)e.tag===0?Qf(Ql.bind(null,e)):wd(Ql.bind(null,e)),Kf(function(){!(X&6)&&Ht()}),n=null;else{switch(Zc(o)){case 1:n=Ga;break;case 4:n=Qc;break;case 16:n=So;break;case 536870912:n=Yc;break;default:n=So}n=ku(n,mu.bind(null,e))}e.callbackPriority=t,e.callbackNode=n}}function mu(e,t){if(vo=-1,yo=0,X&6)throw Error(L(327));var n=e.callbackNode;if(Rn()&&e.callbackNode!==n)return null;var o=No(e,e===ve?ke:0);if(o===0)return null;if(o&30||o&e.expiredLanes||t)t=$o(e,o);else{t=o;var i=X;X|=2;var a=hu();(ve!==e||ke!==t)&&(mt=null,On=de()+500,Jt(e,t));do try{hm();break}catch(l){gu(e,l)}while(!0);as(),Fo.current=a,X=i,ue!==null?t=0:(ve=null,ke=0,t=ge)}if(t!==0){if(t===2&&(i=ia(e),i!==0&&(o=i,t=Pa(e,i))),t===1)throw n=Dr,Jt(e,0),zt(e,o),Be(e,de()),n;if(t===6)zt(e,o);else{if(i=e.current.alternate,!(o&30)&&!mm(i)&&(t=$o(e,o),t===2&&(a=ia(e),a!==0&&(o=a,t=Pa(e,a))),t===1))throw n=Dr,Jt(e,0),zt(e,o),Be(e,de()),n;switch(e.finishedWork=i,e.finishedLanes=o,t){case 0:case 1:throw Error(L(345));case 2:Gt(e,Ie,mt);break;case 3:if(zt(e,o),(o&130023424)===o&&(t=ks+500-de(),10<t)){if(No(e,0)!==0)break;if(i=e.suspendedLanes,(i&o)!==o){_e(),e.pingedLanes|=e.suspendedLanes&i;break}e.timeoutHandle=fa(Gt.bind(null,e,Ie,mt),t);break}Gt(e,Ie,mt);break;case 4:if(zt(e,o),(o&4194240)===o)break;for(t=e.eventTimes,i=-1;0<o;){var s=31-it(o);a=1<<s,s=t[s],s>i&&(i=s),o&=~a}if(o=i,o=de()-o,o=(120>o?120:480>o?480:1080>o?1080:1920>o?1920:3e3>o?3e3:4320>o?4320:1960*fm(o/1960))-o,10<o){e.timeoutHandle=fa(Gt.bind(null,e,Ie,mt),o);break}Gt(e,Ie,mt);break;case 5:Gt(e,Ie,mt);break;default:throw Error(L(329))}}}return Be(e,de()),e.callbackNode===n?mu.bind(null,e):null}function Pa(e,t){var n=hr;return e.current.memoizedState.isDehydrated&&(Jt(e,t).flags|=256),e=$o(e,t),e!==2&&(t=Ie,Ie=n,t!==null&&Da(t)),e}function Da(e){Ie===null?Ie=e:Ie.push.apply(Ie,e)}function mm(e){for(var t=e;;){if(t.flags&16384){var n=t.updateQueue;if(n!==null&&(n=n.stores,n!==null))for(var o=0;o<n.length;o++){var i=n[o],a=i.getSnapshot;i=i.value;try{if(!st(a(),i))return!1}catch{return!1}}}if(n=t.child,t.subtreeFlags&16384&&n!==null)n.return=t,t=n;else{if(t===e)break;for(;t.sibling===null;){if(t.return===null||t.return===e)return!0;t=t.return}t.sibling.return=t.return,t=t.sibling}}return!0}function zt(e,t){for(t&=~bs,t&=~ni,e.suspendedLanes|=t,e.pingedLanes&=~t,e=e.expirationTimes;0<t;){var n=31-it(t),o=1<<n;e[n]=-1,t&=~o}}function Ql(e){if(X&6)throw Error(L(327));Rn();var t=No(e,0);if(!(t&1))return Be(e,de()),null;var n=$o(e,t);if(e.tag!==0&&n===2){var o=ia(e);o!==0&&(t=o,n=Pa(e,o))}if(n===1)throw n=Dr,Jt(e,0),zt(e,t),Be(e,de()),n;if(n===6)throw Error(L(345));return e.finishedWork=e.current.alternate,e.finishedLanes=t,Gt(e,Ie,mt),Be(e,de()),null}function js(e,t){var n=X;X|=1;try{return e(t)}finally{X=n,X===0&&(On=de()+500,Zo&&Ht())}}function rn(e){Rt!==null&&Rt.tag===0&&!(X&6)&&Rn();var t=X;X|=1;var n=Je.transition,o=Z;try{if(Je.transition=null,Z=1,e)return e()}finally{Z=o,Je.transition=n,X=t,!(X&6)&&Ht()}}function ws(){Ue=Nn.current,re(Nn)}function Jt(e,t){e.finishedWork=null,e.finishedLanes=0;var n=e.timeoutHandle;if(n!==-1&&(e.timeoutHandle=-1,Hf(n)),ue!==null)for(n=ue.return;n!==null;){var o=n;switch(rs(o),o.tag){case 1:o=o.type.childContextTypes,o!=null&&_o();break;case 3:Mn(),re(Ae),re(Te),ps();break;case 5:us(o);break;case 4:Mn();break;case 13:re(ie);break;case 19:re(ie);break;case 10:ss(o.type._context);break;case 22:case 23:ws()}n=n.return}if(ve=e,ue=e=Bt(e.current,null),ke=Ue=t,ge=0,Dr=null,bs=ni=nn=0,Ie=hr=null,Qt!==null){for(t=0;t<Qt.length;t++)if(n=Qt[t],o=n.interleaved,o!==null){n.interleaved=null;var i=o.next,a=n.pending;if(a!==null){var s=a.next;a.next=i,o.next=s}n.pending=o}Qt=null}return e}function gu(e,t){do{var n=ue;try{if(as(),go.current=Bo,Oo){for(var o=ae.memoizedState;o!==null;){var i=o.queue;i!==null&&(i.pending=null),o=o.next}Oo=!1}if(tn=0,xe=me=ae=null,mr=!1,Rr=0,ys.current=null,n===null||n.return===null){ge=1,Dr=t,ue=null;break}e:{var a=e,s=n.return,l=n,c=t;if(t=ke,l.flags|=32768,c!==null&&typeof c=="object"&&typeof c.then=="function"){var d=c,h=l,f=h.tag;if(!(h.mode&1)&&(f===0||f===11||f===15)){var g=h.alternate;g?(h.updateQueue=g.updateQueue,h.memoizedState=g.memoizedState,h.lanes=g.lanes):(h.updateQueue=null,h.memoizedState=null)}var y=Ml(s);if(y!==null){y.flags&=-257,Al(y,s,l,a,t),y.mode&1&&Il(a,d,t),t=y,c=d;var v=t.updateQueue;if(v===null){var b=new Set;b.add(c),t.updateQueue=b}else v.add(c);break e}else{if(!(t&1)){Il(a,d,t),Ss();break e}c=Error(L(426))}}else if(oe&&l.mode&1){var z=Ml(s);if(z!==null){!(z.flags&65536)&&(z.flags|=256),Al(z,s,l,a,t),os(An(c,l));break e}}a=c=An(c,l),ge!==4&&(ge=2),hr===null?hr=[a]:hr.push(a),a=s;do{switch(a.tag){case 3:a.flags|=65536,t&=-t,a.lanes|=t;var p=Zd(a,c,t);zl(a,p);break e;case 1:l=c;var u=a.type,m=a.stateNode;if(!(a.flags&128)&&(typeof u.getDerivedStateFromError=="function"||m!==null&&typeof m.componentDidCatch=="function"&&(At===null||!At.has(m)))){a.flags|=65536,t&=-t,a.lanes|=t;var j=qd(a,l,t);zl(a,j);break e}}a=a.return}while(a!==null)}vu(n)}catch(T){t=T,ue===n&&n!==null&&(ue=n=n.return);continue}break}while(!0)}function hu(){var e=Fo.current;return Fo.current=Bo,e===null?Bo:e}function Ss(){(ge===0||ge===3||ge===2)&&(ge=4),ve===null||!(nn&268435455)&&!(ni&268435455)||zt(ve,ke)}function $o(e,t){var n=X;X|=2;var o=hu();(ve!==e||ke!==t)&&(mt=null,Jt(e,t));do try{gm();break}catch(i){gu(e,i)}while(!0);if(as(),X=n,Fo.current=o,ue!==null)throw Error(L(261));return ve=null,ke=0,ge}function gm(){for(;ue!==null;)xu(ue)}function hm(){for(;ue!==null&&!Fp();)xu(ue)}function xu(e){var t=bu(e.alternate,e,Ue);e.memoizedProps=e.pendingProps,t===null?vu(e):ue=t,ys.current=null}function vu(e){var t=e;do{var n=t.alternate;if(e=t.return,t.flags&32768){if(n=cm(n,t),n!==null){n.flags&=32767,ue=n;return}if(e!==null)e.flags|=32768,e.subtreeFlags=0,e.deletions=null;else{ge=6,ue=null;return}}else if(n=lm(n,t,Ue),n!==null){ue=n;return}if(t=t.sibling,t!==null){ue=t;return}ue=t=e}while(t!==null);ge===0&&(ge=5)}function Gt(e,t,n){var o=Z,i=Je.transition;try{Je.transition=null,Z=1,xm(e,t,n,o)}finally{Je.transition=i,Z=o}return null}function xm(e,t,n,o){do Rn();while(Rt!==null);if(X&6)throw Error(L(327));n=e.finishedWork;var i=e.finishedLanes;if(n===null)return null;if(e.finishedWork=null,e.finishedLanes=0,n===e.current)throw Error(L(177));e.callbackNode=null,e.callbackPriority=0;var a=n.lanes|n.childLanes;if(Yp(e,a),e===ve&&(ue=ve=null,ke=0),!(n.subtreeFlags&2064)&&!(n.flags&2064)||no||(no=!0,ku(So,function(){return Rn(),null})),a=(n.flags&15990)!==0,n.subtreeFlags&15990||a){a=Je.transition,Je.transition=null;var s=Z;Z=1;var l=X;X|=4,ys.current=null,um(e,n),pu(n,e),Af(ua),Co=!!da,ua=da=null,e.current=n,pm(n),Up(),X=l,Z=s,Je.transition=a}else e.current=n;if(no&&(no=!1,Rt=e,Wo=i),a=e.pendingLanes,a===0&&(At=null),Hp(n.stateNode),Be(e,de()),t!==null)for(o=e.onRecoverableError,n=0;n<t.length;n++)i=t[n],o(i.value,{componentStack:i.stack,digest:i.digest});if(Uo)throw Uo=!1,e=Ra,Ra=null,e;return Wo&1&&e.tag!==0&&Rn(),a=e.pendingLanes,a&1?e===La?xr++:(xr=0,La=e):xr=0,Ht(),null}function Rn(){if(Rt!==null){var e=Zc(Wo),t=Je.transition,n=Z;try{if(Je.transition=null,Z=16>e?16:e,Rt===null)var o=!1;else{if(e=Rt,Rt=null,Wo=0,X&6)throw Error(L(331));var i=X;for(X|=4,A=e.current;A!==null;){var a=A,s=a.child;if(A.flags&16){var l=a.deletions;if(l!==null){for(var c=0;c<l.length;c++){var d=l[c];for(A=d;A!==null;){var h=A;switch(h.tag){case 0:case 11:case 15:gr(8,h,a)}var f=h.child;if(f!==null)f.return=h,A=f;else for(;A!==null;){h=A;var g=h.sibling,y=h.return;if(cu(h),h===d){A=null;break}if(g!==null){g.return=y,A=g;break}A=y}}}var v=a.alternate;if(v!==null){var b=v.child;if(b!==null){v.child=null;do{var z=b.sibling;b.sibling=null,b=z}while(b!==null)}}A=a}}if(a.subtreeFlags&2064&&s!==null)s.return=a,A=s;else e:for(;A!==null;){if(a=A,a.flags&2048)switch(a.tag){case 0:case 11:case 15:gr(9,a,a.return)}var p=a.sibling;if(p!==null){p.return=a.return,A=p;break e}A=a.return}}var u=e.current;for(A=u;A!==null;){s=A;var m=s.child;if(s.subtreeFlags&2064&&m!==null)m.return=s,A=m;else e:for(s=u;A!==null;){if(l=A,l.flags&2048)try{switch(l.tag){case 0:case 11:case 15:ti(9,l)}}catch(T){ce(l,l.return,T)}if(l===s){A=null;break e}var j=l.sibling;if(j!==null){j.return=l.return,A=j;break e}A=l.return}}if(X=i,Ht(),ut&&typeof ut.onPostCommitFiberRoot=="function")try{ut.onPostCommitFiberRoot(Go,e)}catch{}o=!0}return o}finally{Z=n,Je.transition=t}}return!1}function Yl(e,t,n){t=An(n,t),t=Zd(e,t,1),e=Mt(e,t,1),t=_e(),e!==null&&(Mr(e,1,t),Be(e,t))}function ce(e,t,n){if(e.tag===3)Yl(e,e,n);else for(;t!==null;){if(t.tag===3){Yl(t,e,n);break}else if(t.tag===1){var o=t.stateNode;if(typeof t.type.getDerivedStateFromError=="function"||typeof o.componentDidCatch=="function"&&(At===null||!At.has(o))){e=An(n,e),e=qd(t,e,1),t=Mt(t,e,1),e=_e(),t!==null&&(Mr(t,1,e),Be(t,e));break}}t=t.return}}function vm(e,t,n){var o=e.pingCache;o!==null&&o.delete(t),t=_e(),e.pingedLanes|=e.suspendedLanes&n,ve===e&&(ke&n)===n&&(ge===4||ge===3&&(ke&130023424)===ke&&500>de()-ks?Jt(e,0):bs|=n),Be(e,t)}function yu(e,t){t===0&&(e.mode&1?(t=Vr,Vr<<=1,!(Vr&130023424)&&(Vr=4194304)):t=1);var n=_e();e=kt(e,t),e!==null&&(Mr(e,t,n),Be(e,n))}function ym(e){var t=e.memoizedState,n=0;t!==null&&(n=t.retryLane),yu(e,n)}function bm(e,t){var n=0;switch(e.tag){case 13:var o=e.stateNode,i=e.memoizedState;i!==null&&(n=i.retryLane);break;case 19:o=e.stateNode;break;default:throw Error(L(314))}o!==null&&o.delete(t),yu(e,n)}var bu;bu=function(e,t,n){if(e!==null)if(e.memoizedProps!==t.pendingProps||Ae.current)Me=!0;else{if(!(e.lanes&n)&&!(t.flags&128))return Me=!1,sm(e,t,n);Me=!!(e.flags&131072)}else Me=!1,oe&&t.flags&1048576&&Sd(t,Po,t.index);switch(t.lanes=0,t.tag){case 2:var o=t.type;xo(e,t),e=t.pendingProps;var i=Pn(t,Te.current);_n(t,n),i=ms(null,t,o,e,i,n);var a=gs();return t.flags|=1,typeof i=="object"&&i!==null&&typeof i.render=="function"&&i.$$typeof===void 0?(t.tag=1,t.memoizedState=null,t.updateQueue=null,Oe(o)?(a=!0,Ro(t)):a=!1,t.memoizedState=i.state!==null&&i.state!==void 0?i.state:null,cs(t),i.updater=ei,t.stateNode=i,i._reactInternals=t,ba(t,o,e,n),t=wa(null,t,o,!0,a,n)):(t.tag=0,oe&&a&&ns(t),ze(null,t,i,n),t=t.child),t;case 16:o=t.elementType;e:{switch(xo(e,t),e=t.pendingProps,i=o._init,o=i(o._payload),t.type=o,i=t.tag=jm(o),e=nt(o,e),i){case 0:t=ja(null,t,o,e,n);break e;case 1:t=Fl(null,t,o,e,n);break e;case 11:t=Ol(null,t,o,e,n);break e;case 14:t=Bl(null,t,o,nt(o.type,e),n);break e}throw Error(L(306,o,""))}return t;case 0:return o=t.type,i=t.pendingProps,i=t.elementType===o?i:nt(o,i),ja(e,t,o,i,n);case 1:return o=t.type,i=t.pendingProps,i=t.elementType===o?i:nt(o,i),Fl(e,t,o,i,n);case 3:e:{if(ru(t),e===null)throw Error(L(387));o=t.pendingProps,a=t.memoizedState,i=a.element,_d(e,t),Mo(t,o,null,n);var s=t.memoizedState;if(o=s.element,a.isDehydrated)if(a={element:o,isDehydrated:!1,cache:s.cache,pendingSuspenseBoundaries:s.pendingSuspenseBoundaries,transitions:s.transitions},t.updateQueue.baseState=a,t.memoizedState=a,t.flags&256){i=An(Error(L(423)),t),t=Ul(e,t,o,n,i);break e}else if(o!==i){i=An(Error(L(424)),t),t=Ul(e,t,o,n,i);break e}else for(We=It(t.stateNode.containerInfo.firstChild),$e=t,oe=!0,ot=null,n=Ed(t,null,o,n),t.child=n;n;)n.flags=n.flags&-3|4096,n=n.sibling;else{if(Dn(),o===i){t=jt(e,t,n);break e}ze(e,t,o,n)}t=t.child}return t;case 5:return Rd(t),e===null&&xa(t),o=t.type,i=t.pendingProps,a=e!==null?e.memoizedProps:null,s=i.children,pa(o,i)?s=null:a!==null&&pa(o,a)&&(t.flags|=32),nu(e,t),ze(e,t,s,n),t.child;case 6:return e===null&&xa(t),null;case 13:return ou(e,t,n);case 4:return ds(t,t.stateNode.containerInfo),o=t.pendingProps,e===null?t.child=In(t,null,o,n):ze(e,t,o,n),t.child;case 11:return o=t.type,i=t.pendingProps,i=t.elementType===o?i:nt(o,i),Ol(e,t,o,i,n);case 7:return ze(e,t,t.pendingProps,n),t.child;case 8:return ze(e,t,t.pendingProps.children,n),t.child;case 12:return ze(e,t,t.pendingProps.children,n),t.child;case 10:e:{if(o=t.type._context,i=t.pendingProps,a=t.memoizedProps,s=i.value,ee(Do,o._currentValue),o._currentValue=s,a!==null)if(st(a.value,s)){if(a.children===i.children&&!Ae.current){t=jt(e,t,n);break e}}else for(a=t.child,a!==null&&(a.return=t);a!==null;){var l=a.dependencies;if(l!==null){s=a.child;for(var c=l.firstContext;c!==null;){if(c.context===o){if(a.tag===1){c=vt(-1,n&-n),c.tag=2;var d=a.updateQueue;if(d!==null){d=d.shared;var h=d.pending;h===null?c.next=c:(c.next=h.next,h.next=c),d.pending=c}}a.lanes|=n,c=a.alternate,c!==null&&(c.lanes|=n),va(a.return,n,t),l.lanes|=n;break}c=c.next}}else if(a.tag===10)s=a.type===t.type?null:a.child;else if(a.tag===18){if(s=a.return,s===null)throw Error(L(341));s.lanes|=n,l=s.alternate,l!==null&&(l.lanes|=n),va(s,n,t),s=a.sibling}else s=a.child;if(s!==null)s.return=a;else for(s=a;s!==null;){if(s===t){s=null;break}if(a=s.sibling,a!==null){a.return=s.return,s=a;break}s=s.return}a=s}ze(e,t,i.children,n),t=t.child}return t;case 9:return i=t.type,o=t.pendingProps.children,_n(t,n),i=Ze(i),o=o(i),t.flags|=1,ze(e,t,o,n),t.child;case 14:return o=t.type,i=nt(o,t.pendingProps),i=nt(o.type,i),Bl(e,t,o,i,n);case 15:return eu(e,t,t.type,t.pendingProps,n);case 17:return o=t.type,i=t.pendingProps,i=t.elementType===o?i:nt(o,i),xo(e,t),t.tag=1,Oe(o)?(e=!0,Ro(t)):e=!1,_n(t,n),Jd(t,o,i),ba(t,o,i,n),wa(null,t,o,!0,e,n);case 19:return iu(e,t,n);case 22:return tu(e,t,n)}throw Error(L(156,t.tag))};function ku(e,t){return Xc(e,t)}function km(e,t,n,o){this.tag=e,this.key=n,this.sibling=this.child=this.return=this.stateNode=this.type=this.elementType=null,this.index=0,this.ref=null,this.pendingProps=t,this.dependencies=this.memoizedState=this.updateQueue=this.memoizedProps=null,this.mode=o,this.subtreeFlags=this.flags=0,this.deletions=null,this.childLanes=this.lanes=0,this.alternate=null}function Ye(e,t,n,o){return new km(e,t,n,o)}function Ns(e){return e=e.prototype,!(!e||!e.isReactComponent)}function jm(e){if(typeof e=="function")return Ns(e)?1:0;if(e!=null){if(e=e.$$typeof,e===Ha)return 11;if(e===Ka)return 14}return 2}function Bt(e,t){var n=e.alternate;return n===null?(n=Ye(e.tag,t,e.key,e.mode),n.elementType=e.elementType,n.type=e.type,n.stateNode=e.stateNode,n.alternate=e,e.alternate=n):(n.pendingProps=t,n.type=e.type,n.flags=0,n.subtreeFlags=0,n.deletions=null),n.flags=e.flags&14680064,n.childLanes=e.childLanes,n.lanes=e.lanes,n.child=e.child,n.memoizedProps=e.memoizedProps,n.memoizedState=e.memoizedState,n.updateQueue=e.updateQueue,t=e.dependencies,n.dependencies=t===null?null:{lanes:t.lanes,firstContext:t.firstContext},n.sibling=e.sibling,n.index=e.index,n.ref=e.ref,n}function bo(e,t,n,o,i,a){var s=2;if(o=e,typeof e=="function")Ns(e)&&(s=1);else if(typeof e=="string")s=5;else e:switch(e){case gn:return Zt(n.children,i,a,t);case $a:s=8,i|=8;break;case Hi:return e=Ye(12,n,t,i|2),e.elementType=Hi,e.lanes=a,e;case Ki:return e=Ye(13,n,t,i),e.elementType=Ki,e.lanes=a,e;case Vi:return e=Ye(19,n,t,i),e.elementType=Vi,e.lanes=a,e;case Rc:return ri(n,i,a,t);default:if(typeof e=="object"&&e!==null)switch(e.$$typeof){case zc:s=10;break e;case _c:s=9;break e;case Ha:s=11;break e;case Ka:s=14;break e;case Ct:s=16,o=null;break e}throw Error(L(130,e==null?e:typeof e,""))}return t=Ye(s,n,t,i),t.elementType=e,t.type=o,t.lanes=a,t}function Zt(e,t,n,o){return e=Ye(7,e,o,t),e.lanes=n,e}function ri(e,t,n,o){return e=Ye(22,e,o,t),e.elementType=Rc,e.lanes=n,e.stateNode={isHidden:!1},e}function Ai(e,t,n){return e=Ye(6,e,null,t),e.lanes=n,e}function Oi(e,t,n){return t=Ye(4,e.children!==null?e.children:[],e.key,t),t.lanes=n,t.stateNode={containerInfo:e.containerInfo,pendingChildren:null,implementation:e.implementation},t}function wm(e,t,n,o,i){this.tag=t,this.containerInfo=e,this.finishedWork=this.pingCache=this.current=this.pendingChildren=null,this.timeoutHandle=-1,this.callbackNode=this.pendingContext=this.context=null,this.callbackPriority=0,this.eventTimes=vi(0),this.expirationTimes=vi(-1),this.entangledLanes=this.finishedLanes=this.mutableReadLanes=this.expiredLanes=this.pingedLanes=this.suspendedLanes=this.pendingLanes=0,this.entanglements=vi(0),this.identifierPrefix=o,this.onRecoverableError=i,this.mutableSourceEagerHydrationData=null}function Cs(e,t,n,o,i,a,s,l,c){return e=new wm(e,t,n,l,c),t===1?(t=1,a===!0&&(t|=8)):t=0,a=Ye(3,null,null,t),e.current=a,a.stateNode=e,a.memoizedState={element:o,isDehydrated:n,cache:null,transitions:null,pendingSuspenseBoundaries:null},cs(a),e}function Sm(e,t,n){var o=3<arguments.length&&arguments[3]!==void 0?arguments[3]:null;return{$$typeof:mn,key:o==null?null:""+o,children:e,containerInfo:t,implementation:n}}function ju(e){if(!e)return Ut;e=e._reactInternals;e:{if(an(e)!==e||e.tag!==1)throw Error(L(170));var t=e;do{switch(t.tag){case 3:t=t.stateNode.context;break e;case 1:if(Oe(t.type)){t=t.stateNode.__reactInternalMemoizedMergedChildContext;break e}}t=t.return}while(t!==null);throw Error(L(171))}if(e.tag===1){var n=e.type;if(Oe(n))return jd(e,n,t)}return t}function wu(e,t,n,o,i,a,s,l,c){return e=Cs(n,o,!0,e,i,a,s,l,c),e.context=ju(null),n=e.current,o=_e(),i=Ot(n),a=vt(o,i),a.callback=t??null,Mt(n,a,i),e.current.lanes=i,Mr(e,i,o),Be(e,o),e}function oi(e,t,n,o){var i=t.current,a=_e(),s=Ot(i);return n=ju(n),t.context===null?t.context=n:t.pendingContext=n,t=vt(a,s),t.payload={element:e},o=o===void 0?null:o,o!==null&&(t.callback=o),e=Mt(i,t,s),e!==null&&(at(e,i,s,a),mo(e,i,s)),s}function Ho(e){if(e=e.current,!e.child)return null;switch(e.child.tag){case 5:return e.child.stateNode;default:return e.child.stateNode}}function Jl(e,t){if(e=e.memoizedState,e!==null&&e.dehydrated!==null){var n=e.retryLane;e.retryLane=n!==0&&n<t?n:t}}function Ts(e,t){Jl(e,t),(e=e.alternate)&&Jl(e,t)}function Nm(){return null}var Su=typeof reportError=="function"?reportError:function(e){console.error(e)};function Es(e){this._internalRoot=e}ii.prototype.render=Es.prototype.render=function(e){var t=this._internalRoot;if(t===null)throw Error(L(409));oi(e,t,null,null)};ii.prototype.unmount=Es.prototype.unmount=function(){var e=this._internalRoot;if(e!==null){this._internalRoot=null;var t=e.containerInfo;rn(function(){oi(null,e,null,null)}),t[bt]=null}};function ii(e){this._internalRoot=e}ii.prototype.unstable_scheduleHydration=function(e){if(e){var t=td();e={blockedOn:null,target:e,priority:t};for(var n=0;n<Et.length&&t!==0&&t<Et[n].priority;n++);Et.splice(n,0,e),n===0&&rd(e)}};function zs(e){return!(!e||e.nodeType!==1&&e.nodeType!==9&&e.nodeType!==11)}function ai(e){return!(!e||e.nodeType!==1&&e.nodeType!==9&&e.nodeType!==11&&(e.nodeType!==8||e.nodeValue!==" react-mount-point-unstable "))}function Zl(){}function Cm(e,t,n,o,i){if(i){if(typeof o=="function"){var a=o;o=function(){var d=Ho(s);a.call(d)}}var s=wu(t,o,e,0,null,!1,!1,"",Zl);return e._reactRootContainer=s,e[bt]=s.current,Cr(e.nodeType===8?e.parentNode:e),rn(),s}for(;i=e.lastChild;)e.removeChild(i);if(typeof o=="function"){var l=o;o=function(){var d=Ho(c);l.call(d)}}var c=Cs(e,0,!1,null,null,!1,!1,"",Zl);return e._reactRootContainer=c,e[bt]=c.current,Cr(e.nodeType===8?e.parentNode:e),rn(function(){oi(t,c,n,o)}),c}function si(e,t,n,o,i){var a=n._reactRootContainer;if(a){var s=a;if(typeof i=="function"){var l=i;i=function(){var c=Ho(s);l.call(c)}}oi(t,s,e,i)}else s=Cm(n,t,e,i,o);return Ho(s)}qc=function(e){switch(e.tag){case 3:var t=e.stateNode;if(t.current.memoizedState.isDehydrated){var n=ar(t.pendingLanes);n!==0&&(Xa(t,n|1),Be(t,de()),!(X&6)&&(On=de()+500,Ht()))}break;case 13:rn(function(){var o=kt(e,1);if(o!==null){var i=_e();at(o,e,1,i)}}),Ts(e,1)}};Qa=function(e){if(e.tag===13){var t=kt(e,134217728);if(t!==null){var n=_e();at(t,e,134217728,n)}Ts(e,134217728)}};ed=function(e){if(e.tag===13){var t=Ot(e),n=kt(e,t);if(n!==null){var o=_e();at(n,e,t,o)}Ts(e,t)}};td=function(){return Z};nd=function(e,t){var n=Z;try{return Z=e,t()}finally{Z=n}};na=function(e,t,n){switch(t){case"input":if(Qi(e,n),t=n.name,n.type==="radio"&&t!=null){for(n=e;n.parentNode;)n=n.parentNode;for(n=n.querySelectorAll("input[name="+JSON.stringify(""+t)+'][type="radio"]'),t=0;t<n.length;t++){var o=n[t];if(o!==e&&o.form===e.form){var i=Jo(o);if(!i)throw Error(L(90));Pc(o),Qi(o,i)}}}break;case"textarea":Ic(e,n);break;case"select":t=n.value,t!=null&&Cn(e,!!n.multiple,t,!1)}};Wc=js;$c=rn;var Tm={usingClientEntryPoint:!1,Events:[Or,yn,Jo,Fc,Uc,js]},tr={findFiberByHostInstance:Xt,bundleType:0,version:"18.3.1",rendererPackageName:"react-dom"},Em={bundleType:tr.bundleType,version:tr.version,rendererPackageName:tr.rendererPackageName,rendererConfig:tr.rendererConfig,overrideHookState:null,overrideHookStateDeletePath:null,overrideHookStateRenamePath:null,overrideProps:null,overridePropsDeletePath:null,overridePropsRenamePath:null,setErrorHandler:null,setSuspenseHandler:null,scheduleUpdate:null,currentDispatcherRef:wt.ReactCurrentDispatcher,findHostInstanceByFiber:function(e){return e=Vc(e),e===null?null:e.stateNode},findFiberByHostInstance:tr.findFiberByHostInstance||Nm,findHostInstancesForRefresh:null,scheduleRefresh:null,scheduleRoot:null,setRefreshHandler:null,getCurrentFiber:null,reconcilerVersion:"18.3.1-next-f1338f8080-20240426"};if(typeof __REACT_DEVTOOLS_GLOBAL_HOOK__<"u"){var ro=__REACT_DEVTOOLS_GLOBAL_HOOK__;if(!ro.isDisabled&&ro.supportsFiber)try{Go=ro.inject(Em),ut=ro}catch{}}Ke.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED=Tm;Ke.createPortal=function(e,t){var n=2<arguments.length&&arguments[2]!==void 0?arguments[2]:null;if(!zs(t))throw Error(L(200));return Sm(e,t,null,n)};Ke.createRoot=function(e,t){if(!zs(e))throw Error(L(299));var n=!1,o="",i=Su;return t!=null&&(t.unstable_strictMode===!0&&(n=!0),t.identifierPrefix!==void 0&&(o=t.identifierPrefix),t.onRecoverableError!==void 0&&(i=t.onRecoverableError)),t=Cs(e,1,!1,null,null,n,!1,o,i),e[bt]=t.current,Cr(e.nodeType===8?e.parentNode:e),new Es(t)};Ke.findDOMNode=function(e){if(e==null)return null;if(e.nodeType===1)return e;var t=e._reactInternals;if(t===void 0)throw typeof e.render=="function"?Error(L(188)):(e=Object.keys(e).join(","),Error(L(268,e)));return e=Vc(t),e=e===null?null:e.stateNode,e};Ke.flushSync=function(e){return rn(e)};Ke.hydrate=function(e,t,n){if(!ai(t))throw Error(L(200));return si(null,e,t,!0,n)};Ke.hydrateRoot=function(e,t,n){if(!zs(e))throw Error(L(405));var o=n!=null&&n.hydratedSources||null,i=!1,a="",s=Su;if(n!=null&&(n.unstable_strictMode===!0&&(i=!0),n.identifierPrefix!==void 0&&(a=n.identifierPrefix),n.onRecoverableError!==void 0&&(s=n.onRecoverableError)),t=wu(t,null,e,1,n??null,i,!1,a,s),e[bt]=t.current,Cr(e),o)for(e=0;e<o.length;e++)n=o[e],i=n._getVersion,i=i(n._source),t.mutableSourceEagerHydrationData==null?t.mutableSourceEagerHydrationData=[n,i]:t.mutableSourceEagerHydrationData.push(n,i);return new ii(t)};Ke.render=function(e,t,n){if(!ai(t))throw Error(L(200));return si(null,e,t,!1,n)};Ke.unmountComponentAtNode=function(e){if(!ai(e))throw Error(L(40));return e._reactRootContainer?(rn(function(){si(null,null,e,!1,function(){e._reactRootContainer=null,e[bt]=null})}),!0):!1};Ke.unstable_batchedUpdates=js;Ke.unstable_renderSubtreeIntoContainer=function(e,t,n,o){if(!ai(n))throw Error(L(200));if(e==null||e._reactInternals===void 0)throw Error(L(38));return si(e,t,n,!1,o)};Ke.version="18.3.1-next-f1338f8080-20240426";function Nu(){if(!(typeof __REACT_DEVTOOLS_GLOBAL_HOOK__>"u"||typeof __REACT_DEVTOOLS_GLOBAL_HOOK__.checkDCE!="function"))try{__REACT_DEVTOOLS_GLOBAL_HOOK__.checkDCE(Nu)}catch(e){console.error(e)}}Nu(),Nc.exports=Ke;var zm=Nc.exports,ql=zm;Wi.createRoot=ql.createRoot,Wi.hydrateRoot=ql.hydrateRoot;const _m="attribute vec2 a;void main(){gl_Position=vec4(a,0,1);}",Rm=`
precision highp float;
uniform float t;uniform vec2 r;
float n(vec2 p){return fract(sin(dot(p,vec2(127.1,311.7)))*43758.5453);}
float sn(vec2 p){
  vec2 i=floor(p),f=fract(p),u=f*f*(3.-2.*f);
  return mix(mix(n(i),n(i+vec2(1,0)),u.x),mix(n(i+vec2(0,1)),n(i+vec2(1,1)),u.x),u.y);}
float fbm(vec2 p){float v=0.,a=.5;for(int i=0;i<6;i++){v+=a*sn(p);p=p*2.1+vec2(1.7,9.2);a*=.5;}return v;}
void main(){
  vec2 uv=gl_FragCoord.xy/r;uv.x*=r.x/r.y;
  float tm=t*.075;vec2 p=uv*2.8;
  float v=fbm(p+vec2(tm*.6,tm*.4));
  v+=.5*fbm(p*2.-vec2(tm*.3,tm*.7));
  v+=.25*fbm(p*4.+vec2(tm*.9,-tm*.5));
  float c=fract(v*7.);
  float ln=smoothstep(0.,.04,c)*(1.-smoothstep(.06,.12,c));
  float th=smoothstep(0.,.01,c)*(1.-smoothstep(.01,.03,c));
  vec3 deep=vec3(.015,.035,.09),mid=vec3(.04,.13,.26),acc=vec3(.10,.35,.60),br=vec3(.28,.70,.94);
  vec3 base=mix(deep,mid,v);base=mix(base,acc,v*v*.55);
  vec3 col=mix(base,acc*1.35,ln*.52);
  col=mix(col,br,th*.78);
  col*=1.-length(uv-.5)*1.35*.52;
  gl_FragColor=vec4(col,1.);}
`;function Lm(){const e=x.useRef(null);return x.useEffect(()=>{const t=e.current;if(!t)return;const n=t.getContext("webgl");if(!n)return;const o=n.createShader(n.VERTEX_SHADER);n.shaderSource(o,_m),n.compileShader(o);const i=n.createShader(n.FRAGMENT_SHADER);n.shaderSource(i,Rm),n.compileShader(i);const a=n.createProgram();n.attachShader(a,o),n.attachShader(a,i),n.linkProgram(a),n.useProgram(a);const s=n.createBuffer();n.bindBuffer(n.ARRAY_BUFFER,s),n.bufferData(n.ARRAY_BUFFER,new Float32Array([-1,-1,1,-1,-1,1,1,1]),n.STATIC_DRAW);const l=n.getAttribLocation(a,"a");n.enableVertexAttribArray(l),n.vertexAttribPointer(l,2,n.FLOAT,!1,0,0);const c=n.getUniformLocation(a,"t"),d=n.getUniformLocation(a,"r"),h=Date.now();function f(){n.uniform1f(c,(Date.now()-h)/1e3),n.uniform2f(d,t.width,t.height),n.drawArrays(n.TRIANGLE_STRIP,0,4),requestAnimationFrame(f)}function g(){t.width=window.innerWidth*(window.devicePixelRatio||1),t.height=window.innerHeight*(window.devicePixelRatio||1),n.viewport(0,0,t.width,t.height)}g(),window.addEventListener("resize",g),f()},[]),r.jsx("canvas",{ref:e,style:{position:"fixed",inset:0,width:"100%",height:"100%",zIndex:0}})}function Pm({onOpenLogin:e}){return r.jsxs("div",{style:{width:"100vw",height:"100vh",overflow:"hidden",background:"#0b0e11",color:"#e2eaf4",fontFamily:"DM Sans, sans-serif",position:"relative"},children:[r.jsx("style",{children:`
        * { box-sizing: border-box; margin: 0; padding: 0; }
        html,body { width: 100%; height: 100%; overflow: hidden; background: #0b0e11; color: #e2eaf4; }
        .lp-nav { position: relative; z-index: 10; display: flex; align-items: center; height: 72px; padding: 0 40px; background: rgba(3,8,18,0.78); border-bottom: 1px solid rgba(60,140,220,0.14); backdrop-filter: blur(24px); animation: fadeDown 0.5s ease both; }
        @keyframes fadeDown { from { opacity: 0; transform: translateY(-14px); } to { opacity: 1; transform: translateY(0); } }
        .lp-logo { display: flex; align-items: center; gap: 12px; margin-right: 44px; user-select: none; flex-shrink: 0; text-decoration: none; cursor: pointer; }
        .logo-icon { width: 40px; height: 40px; flex-shrink: 0; mix-blend-mode: screen; opacity: 0.92; }
        .logo-wordmark { font-family: 'Orbitron', sans-serif; font-size: 1.18rem; font-weight: 800; letter-spacing: 0.10em; color: #e8f4ff; line-height: 1; }
        .logo-wordmark em { color: #4db8ff; font-style: normal; }
        .lp-nav-items { display: flex; align-items: center; gap: 0; flex: 1; }
        .lp-nav-btn { background: none; border: none; color: rgba(195,218,240,0.80); font-family: 'Rajdhani', sans-serif; font-size: 1.08rem; font-weight: 600; letter-spacing: 0.06em; padding: 8px 20px; border-radius: 7px; cursor: pointer; display: flex; align-items: center; gap: 6px; transition: color 0.18s; white-space: nowrap; position: relative; }
        .lp-nav-btn::after { content: ''; position: absolute; bottom: 5px; left: 50%; transform: translateX(-50%); width: 0; height: 2px; background: #4db8ff; border-radius: 2px; transition: width 0.22s ease; }
        .lp-nav-btn:hover::after { width: 55%; }
        .lp-nav-btn:hover { color: #d8eeff; }
        .lp-nav-btn.dim { opacity: 0.36; cursor: default; pointer-events: none; }
        .lp-nav-btn--premium { background: none !important; box-shadow: none !important; }
        .lp-nav-btn--premium::after { display: none !important; }
        .lp-premium-label {
          background: linear-gradient(90deg, #4db8ff, #2962ff, #a040ff, #00d4ff, #4db8ff);
          background-size: 300% auto;
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
          animation: lp-premium-shimmer 4s linear infinite, lp-premium-txt-glow 3s ease-in-out infinite;
        }
        @keyframes lp-premium-shimmer { 0%{background-position:0% 50%} 50%{background-position:100% 50%} 100%{background-position:0% 50%} }
        @keyframes lp-premium-txt-glow {
          0%,100% { text-shadow: 0 0 8px rgba(77,184,255,0.9), 0 0 16px rgba(41,98,255,0.6); }
          33%     { text-shadow: 0 0 8px rgba(160,64,255,0.9), 0 0 16px rgba(120,40,220,0.6); }
          66%     { text-shadow: 0 0 8px rgba(0,200,255,0.9), 0 0 16px rgba(0,140,255,0.6); }
        }
        .caret { width: 10px; height: auto; flex-shrink: 0; opacity: 0.55; }
        .lp-nav-right { display: flex; align-items: center; gap: 12px; margin-left: auto; }
        .lp-lang-btn { background: none; border: none; color: rgba(175,208,240,0.62); font-family: 'DM Sans', sans-serif; font-size: 0.9rem; padding: 7px 14px; border-radius: 7px; cursor: pointer; display: flex; align-items: center; gap: 7px; transition: background 0.18s, color 0.18s; }
        .lp-lang-btn:hover { background: rgba(77,184,255,0.09); color: #c8e6ff; }
        .lp-enter-btn { position: relative; overflow: hidden; isolation: isolate; padding: 9px 26px; border-radius: 8px; cursor: pointer; font-family: 'Rajdhani', sans-serif; font-size: 1.05rem; font-weight: 700; letter-spacing: 0.10em; color: rgba(185,212,240,0.88); border: 1px solid rgba(77,184,255,0.30); background: rgba(255,255,255,0.04); transition: all 0.2s ease; }
        .lp-enter-btn:hover { background: rgba(77,184,255,0.10); border-color: rgba(77,184,255,0.55); color: #c8e6ff; transform: translateY(-2px); box-shadow: 0 4px 18px rgba(77,184,255,0.18); }
        .btn-ring { position: absolute; inset: -1px; border-radius: 9px; border: 1px solid rgba(77,184,255,0.38); pointer-events: none; }
        .overlay { position: fixed; inset: 0; z-index: 1; background: radial-gradient(ellipse 70% 55% at 50% 42%, rgba(3,8,18,0.04) 0%, rgba(3,8,18,0.52) 100%), linear-gradient(to bottom, rgba(3,8,18,0.62) 0%, rgba(3,8,18,0.08) 45%, rgba(3,8,18,0.76) 100%); }
        .lp-hero { position: relative; z-index: 5; display: flex; align-items: center; justify-content: center; padding: 0 32px; height: calc(100vh - 72px - 44px); animation: fadeUp 0.72s 0.12s ease both; }
        @keyframes fadeUp { from { opacity: 0; transform: translateY(22px); } to { opacity: 1; transform: translateY(0); } }
        .lp-hero-center { display: flex; flex-direction: column; align-items: center; text-align: center; gap: 20px; max-width: 820px; width: 100%; }
        .lp-badge { display: inline-flex; align-items: center; gap: 9px; background: rgba(77,184,255,0.07); border: 1px solid rgba(77,184,255,0.20); color: rgba(145,205,255,0.88); font-family: 'DM Sans', sans-serif; font-size: 0.8rem; font-weight: 500; padding: 5px 18px; border-radius: 100px; letter-spacing: 0.05em; }
        .badge-dot { width: 6px; height: 6px; border-radius: 50%; background: #4db8ff; box-shadow: 0 0 8px #4db8ff; animation: pulse 2s infinite; flex-shrink: 0; }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.30; } }
        .lp-title { display: flex; flex-direction: row; align-items: baseline; justify-content: center; flex-wrap: wrap; gap: 0; font-family: 'Rajdhani', sans-serif; font-size: clamp(2.8rem,6vw,4.9rem); font-weight: 700; line-height: 1.08; letter-spacing: 0.10em; color: #e8f4ff; }
        .title-plain { color: rgba(220,235,255,0.88); text-shadow: 0 0 40px rgba(77,184,255,0.12); }
        .title-gap { width: 1.2em; display: inline-block; }
        .title-accent { font-weight: 700; background: linear-gradient(100deg, #4db8ff 0%, #90d4ff 45%, #4db8ff 100%); background-size: 200% auto; -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; animation: shimmer 4s linear infinite; filter: drop-shadow(0 0 22px rgba(77,184,255,0.40)); }
        @keyframes shimmer { 0% { background-position: 0% center; } 100% { background-position: 200% center; } }
        .subtitle-en { font-family: 'Syne', sans-serif; font-size: clamp(0.92rem,1.7vw,1.2rem); font-weight: 700; color: rgba(130,185,240,0.42); letter-spacing: 0.30em; text-transform: uppercase; margin-top: -6px; }
        .subtitle { font-family: 'DM Sans', sans-serif; font-size: 0.97rem; font-weight: 400; color: rgba(170,205,240,0.60); line-height: 1.78; max-width: 530px; margin-top: -2px; }
        .cta-group { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; justify-content: center; margin-top: 6px; }
        .cta-primary { position: relative; overflow: hidden; isolation: isolate; padding: 14px 36px; border-radius: 9px; cursor: pointer; font-family: 'Rajdhani', sans-serif; font-size: 1.08rem; font-weight: 700; letter-spacing: 0.08em; color: #fff; border: none; background: transparent; display: flex; align-items: center; gap: 10px; }
        .cta-primary::before { content: ''; position: absolute; inset: 0; border-radius: 9px; z-index: -2; background: linear-gradient(125deg, #0d4fa0, #1b70cc, #0a7aff, #0d4fa0); background-size: 300% 300%; animation: btnShift 3s ease infinite; }
        .cta-primary::after { content: ''; position: absolute; inset: 0; border-radius: 9px; z-index: -1; background: linear-gradient(105deg, transparent 35%, rgba(255,255,255,0.16) 50%, transparent 65%); background-size: 200% 100%; animation: btnSweep 2.4s ease infinite; }
        .cta-primary:hover { transform: translateY(-3px); filter: brightness(1.12); box-shadow: 0 0 36px rgba(77,184,255,0.5), 0 8px 32px rgba(10,80,200,0.55); }
        .cta-primary .btn-ring { border-radius: 10px; border-color: rgba(77,184,255,0.48); }
        @keyframes btnShift { 0%, 100% { background-position: 0% 50%; } 50% { background-position: 100% 50%; } }
        @keyframes btnSweep { 0% { background-position: -200% 0; } 100% { background-position: 200% 0; } }
        .cta-secondary { background: rgba(255,255,255,0.04); border: 1px solid rgba(200,220,240,0.14); color: rgba(185,212,240,0.78); font-family: 'Rajdhani', sans-serif; font-size: 1.05rem; font-weight: 600; letter-spacing: 0.06em; padding: 14px 28px; border-radius: 9px; cursor: pointer; transition: all 0.25s ease; }
        .cta-secondary:hover { background: rgba(77,184,255,0.08); border-color: rgba(77,184,255,0.28); color: #c8e6ff; transform: translateY(-1px); }
        .lp-stats { display: flex; gap: 44px; flex-wrap: wrap; justify-content: center; padding-top: 4px; }
        .lp-stat { display: flex; flex-direction: column; align-items: center; gap: 3px; }
        .stat-val { font-family: 'Space Mono', monospace; font-size: 1.45rem; font-weight: 700; color: #4db8ff; letter-spacing: -0.01em; }
        .stat-label { font-family: 'DM Sans', sans-serif; font-size: 0.73rem; font-weight: 400; color: rgba(150,192,235,0.52); letter-spacing: 0.06em; }
        .lp-ticker { display: flex; align-items: center; gap: 14px; width: 100%; max-width: 700px; background: rgba(5,13,30,0.72); border: 1px solid rgba(48,120,200,0.18); border-radius: 10px; padding: 11px 16px; backdrop-filter: blur(16px); overflow: hidden; margin-top: 2px; }
        .ticker-label { display: flex; align-items: center; gap: 7px; font-family: 'Space Mono', monospace; font-size: 0.68rem; font-weight: 700; color: #4db8ff; letter-spacing: 0.07em; flex-shrink: 0; padding-right: 14px; border-right: 1px solid rgba(77,184,255,0.18); }
        .live-dot { width: 6px; height: 6px; border-radius: 50%; background: #4db8ff; box-shadow: 0 0 7px #4db8ff; animation: pulse 1.5s infinite; flex-shrink: 0; }
        .ticker-track { flex: 1; overflow: hidden; }
        .ticker-inner { display: flex; gap: 30px; animation: scroll 20s linear infinite; width: max-content; }
        @keyframes scroll { from { transform: translateX(0); } to { transform: translateX(-50%); } }
        .ticker-item { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
        .ticker-sym { font-family: 'Space Mono', monospace; font-size: 0.76rem; font-weight: 700; color: #c8e6ff; }
        .ticker-tf { font-size: 0.7rem; color: rgba(135,178,220,0.46); font-family: 'DM Sans', sans-serif; }
        .ticker-action { font-family: 'Space Mono', monospace; font-size: 0.68rem; font-weight: 700; padding: 2px 8px; border-radius: 3px; letter-spacing: 0.04em; }
        .long { background: rgba(14,203,129,0.11); color: #0ecb81; border: 1px solid rgba(14,203,129,0.20); }
        .short { background: rgba(246,70,93,0.11); color: #f64; border: 1px solid rgba(246,70,93,0.20); }
        .lp-footer { position: relative; z-index: 5; height: 44px; display: flex; align-items: center; justify-content: center; gap: 14px; font-size: 0.74rem; color: rgba(130,172,218,0.36); border-top: 1px solid rgba(48,120,200,0.08); background: rgba(3,8,18,0.5); font-family: 'DM Sans', sans-serif; animation: fadeUp 0.6s 0.4s ease both; }
        .sep { opacity: 0.35; }
      `}),r.jsx(Lm,{}),r.jsx("div",{className:"overlay"}),r.jsxs("nav",{className:"lp-nav",children:[r.jsxs("div",{className:"lp-logo",children:[r.jsx("img",{className:"logo-icon",src:"https://sc01.alicdn.com/kf/Ac3903ff596f74d1f804452cc4ffff11e4.png",alt:"IKUNANCE Logo"}),r.jsxs("span",{className:"logo-wordmark",children:[r.jsx("em",{children:"I"}),"KUNANCE"]})]}),r.jsxs("div",{className:"lp-nav-items",children:[r.jsx("button",{className:"lp-nav-btn",children:"监控"}),r.jsx("button",{className:"lp-nav-btn",children:"信号"}),r.jsxs("button",{className:"lp-nav-btn",children:["市场",r.jsx("svg",{className:"caret",viewBox:"0 0 10 6",fill:"none",width:"10",children:r.jsx("path",{d:"M1 1l4 4 4-4",stroke:"currentColor",strokeWidth:"1.6",strokeLinecap:"round"})})]}),r.jsx("button",{className:"lp-nav-btn",onClick:()=>e(),children:"社区"}),r.jsx("button",{className:"lp-nav-btn",onClick:()=>e(),children:"指标"}),r.jsx("button",{className:"lp-nav-btn lp-nav-btn--premium",onClick:()=>e(),children:r.jsx("span",{className:"lp-premium-label",children:"会员"})})]}),r.jsxs("div",{className:"lp-nav-right",children:[r.jsxs("button",{className:"lp-lang-btn",children:[r.jsxs("svg",{viewBox:"0 0 20 20",fill:"none",width:"15",height:"15",children:[r.jsx("circle",{cx:"10",cy:"10",r:"8.5",stroke:"currentColor",strokeWidth:"1.2"}),r.jsx("ellipse",{cx:"10",cy:"10",rx:"3.5",ry:"8.5",stroke:"currentColor",strokeWidth:"1.2"}),r.jsx("path",{d:"M1.5 10h17M1.5 6h17M1.5 14h17",stroke:"currentColor",strokeWidth:"1.2",strokeLinecap:"round"})]}),"中文",r.jsx("svg",{viewBox:"0 0 10 6",fill:"none",width:"10",children:r.jsx("path",{d:"M1 1l4 4 4-4",stroke:"currentColor",strokeWidth:"1.5",strokeLinecap:"round"})})]}),r.jsxs("button",{className:"lp-enter-btn",onClick:()=>e(),children:[r.jsx("span",{className:"btn-ring"}),"开始使用"]})]})]}),r.jsx("main",{className:"lp-hero",children:r.jsxs("div",{className:"lp-hero-center",children:[r.jsxs("div",{className:"lp-badge",children:[r.jsx("span",{className:"badge-dot"}),"新一代金融综合监控平台"]}),r.jsxs("h1",{className:"lp-title",children:[r.jsx("span",{className:"title-plain",children:"谋定于深"}),r.jsx("span",{className:"title-gap"}),r.jsx("span",{className:"title-accent",children:"行之以勇"})]}),r.jsx("p",{className:"subtitle-en",children:"Plan deep · Act bold"}),r.jsxs("p",{className:"subtitle",children:["自建信号引擎，直连多家交易所，毫秒级推送。",r.jsx("br",{}),"开放 PineScript 策略平台，让每个人都能成为量化交易者。"]}),r.jsxs("div",{className:"cta-group",children:[r.jsxs("button",{className:"cta-primary",onClick:()=>e(),children:[r.jsx("span",{className:"btn-ring"}),"Start for Free",r.jsx("svg",{viewBox:"0 0 20 20",fill:"none",width:"16",height:"16",children:r.jsx("path",{d:"M4 10h12M11 5l5 5-5 5",stroke:"currentColor",strokeWidth:"1.9",strokeLinecap:"round",strokeLinejoin:"round"})})]}),r.jsx("button",{className:"cta-secondary",children:"探索信号策略"})]}),r.jsxs("div",{className:"lp-stats",children:[r.jsxs("div",{className:"lp-stat",children:[r.jsx("span",{className:"stat-val",children:"7+"}),r.jsx("span",{className:"stat-label",children:"支持交易所"})]}),r.jsxs("div",{className:"lp-stat",children:[r.jsx("span",{className:"stat-val",children:"<50ms"}),r.jsx("span",{className:"stat-label",children:"信号延迟"})]}),r.jsxs("div",{className:"lp-stat",children:[r.jsx("span",{className:"stat-val",children:"24/7"}),r.jsx("span",{className:"stat-label",children:"持续监控"})]}),r.jsxs("div",{className:"lp-stat",children:[r.jsx("span",{className:"stat-val",children:"∞"}),r.jsx("span",{className:"stat-label",children:"自定义策略"})]})]}),r.jsxs("div",{className:"lp-ticker",children:[r.jsxs("div",{className:"ticker-label",children:[r.jsx("span",{className:"live-dot"}),"LIVE"]}),r.jsx("div",{className:"ticker-track",children:r.jsxs("div",{className:"ticker-inner",children:[r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"BTC/USDT"}),r.jsx("span",{className:"ticker-tf",children:"15m"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"ETH/USDT"}),r.jsx("span",{className:"ticker-tf",children:"1h"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"SOL/USDT"}),r.jsx("span",{className:"ticker-tf",children:"15m"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"BNB/USDT"}),r.jsx("span",{className:"ticker-tf",children:"4h"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"DOGE/USDT"}),r.jsx("span",{className:"ticker-tf",children:"15m"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"OP/USDT"}),r.jsx("span",{className:"ticker-tf",children:"1h"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"ARB/USDT"}),r.jsx("span",{className:"ticker-tf",children:"4h"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"AAPL"}),r.jsx("span",{className:"ticker-tf",children:"1h"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"700.HK"}),r.jsx("span",{className:"ticker-tf",children:"4h"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"NVDA"}),r.jsx("span",{className:"ticker-tf",children:"1h"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"TSLA"}),r.jsx("span",{className:"ticker-tf",children:"4h"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"NASDAQ"}),r.jsx("span",{className:"ticker-tf",children:"1d"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"S&P 500"}),r.jsx("span",{className:"ticker-tf",children:"1d"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"BTC/USDT"}),r.jsx("span",{className:"ticker-tf",children:"15m"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"ETH/USDT"}),r.jsx("span",{className:"ticker-tf",children:"1h"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"SOL/USDT"}),r.jsx("span",{className:"ticker-tf",children:"15m"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"BNB/USDT"}),r.jsx("span",{className:"ticker-tf",children:"4h"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"DOGE/USDT"}),r.jsx("span",{className:"ticker-tf",children:"15m"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"OP/USDT"}),r.jsx("span",{className:"ticker-tf",children:"1h"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"ARB/USDT"}),r.jsx("span",{className:"ticker-tf",children:"4h"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"AAPL"}),r.jsx("span",{className:"ticker-tf",children:"1h"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"700.HK"}),r.jsx("span",{className:"ticker-tf",children:"4h"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"NVDA"}),r.jsx("span",{className:"ticker-tf",children:"1h"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"TSLA"}),r.jsx("span",{className:"ticker-tf",children:"4h"}),r.jsx("span",{className:"ticker-action short",children:"SHORT"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"NASDAQ"}),r.jsx("span",{className:"ticker-tf",children:"1d"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]}),r.jsxs("span",{className:"ticker-item",children:[r.jsx("span",{className:"ticker-sym",children:"S&P 500"}),r.jsx("span",{className:"ticker-tf",children:"1d"}),r.jsx("span",{className:"ticker-action long",children:"LONG"})]})]})})]})]})}),r.jsxs("footer",{className:"lp-footer",children:[r.jsx("span",{children:"© 2025 IKUNANCE"}),r.jsx("span",{className:"sep",children:"·"}),r.jsx("span",{children:"隐私政策"}),r.jsx("span",{className:"sep",children:"·"}),r.jsx("span",{children:"服务条款"})]})]})}const Dm="attribute vec2 a;void main(){gl_Position=vec4(a,0,1);}",Im=`
precision highp float;
uniform float t;uniform vec2 r;
float n(vec2 p){return fract(sin(dot(p,vec2(127.1,311.7)))*43758.5453);}
float sn(vec2 p){
  vec2 i=floor(p),f=fract(p),u=f*f*(3.-2.*f);
  return mix(mix(n(i),n(i+vec2(1,0)),u.x),mix(n(i+vec2(0,1)),n(i+vec2(1,1)),u.x),u.y);}
float fbm(vec2 p){float v=0.,a=.5;for(int i=0;i<6;i++){v+=a*sn(p);p=p*2.1+vec2(1.7,9.2);a*=.5;}return v;}
void main(){
  vec2 uv=gl_FragCoord.xy/r;uv.x*=r.x/r.y;
  float tm=t*.075;vec2 p=uv*2.8;
  float v=fbm(p+vec2(tm*.6,tm*.4));
  v+=.5*fbm(p*2.-vec2(tm*.3,tm*.7));
  v+=.25*fbm(p*4.+vec2(tm*.9,-tm*.5));
  float c=fract(v*7.);
  float ln=smoothstep(0.,.04,c)*(1.-smoothstep(.06,.12,c));
  float th=smoothstep(0.,.01,c)*(1.-smoothstep(.01,.03,c));
  vec3 deep=vec3(.015,.035,.09),mid=vec3(.04,.13,.26),acc=vec3(.10,.35,.60),br=vec3(.28,.70,.94);
  vec3 base=mix(deep,mid,v);base=mix(base,acc,v*v*.55);
  vec3 col=mix(base,acc*1.35,ln*.52);
  col=mix(col,br,th*.78);
  col*=1.-length(uv-.5)*1.35*.52;
  gl_FragColor=vec4(col,1.);}
`;function Mm(){const e=x.useRef(null);return x.useEffect(()=>{const t=e.current;if(!t)return;const n=t.getContext("webgl");if(!n)return;const o=n.createShader(n.VERTEX_SHADER);n.shaderSource(o,Dm),n.compileShader(o);const i=n.createShader(n.FRAGMENT_SHADER);n.shaderSource(i,Im),n.compileShader(i);const a=n.createProgram();n.attachShader(a,o),n.attachShader(a,i),n.linkProgram(a),n.useProgram(a);const s=n.createBuffer();n.bindBuffer(n.ARRAY_BUFFER,s),n.bufferData(n.ARRAY_BUFFER,new Float32Array([-1,-1,1,-1,-1,1,1,1]),n.STATIC_DRAW);const l=n.getAttribLocation(a,"a");n.enableVertexAttribArray(l),n.vertexAttribPointer(l,2,n.FLOAT,!1,0,0);const c=n.getUniformLocation(a,"t"),d=n.getUniformLocation(a,"r"),h=Date.now();let f;function g(){n.uniform1f(c,(Date.now()-h)/1e3),n.uniform2f(d,t.width,t.height),n.drawArrays(n.TRIANGLE_STRIP,0,4),f=requestAnimationFrame(g)}function y(){var z;const v=((z=t.parentElement)==null?void 0:z.getBoundingClientRect())||{width:window.innerWidth,height:window.innerHeight},b=window.devicePixelRatio||1;t.width=v.width*b,t.height=v.height*b,n.viewport(0,0,t.width,t.height)}return y(),window.addEventListener("resize",y),g(),()=>{cancelAnimationFrame(f),window.removeEventListener("resize",y)}},[]),r.jsx("canvas",{ref:e,style:{position:"absolute",inset:0,width:"100%",height:"100%"}})}const Am=`
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=Orbitron:wght@700;800&family=DM+Sans:wght@300;400;500;600&display=swap');

.lo-wrap {
  position: fixed; inset: 0; z-index: 99999;
  display: flex; overflow: hidden;
  font-family: 'DM Sans', sans-serif;
}

/* ── 左侧 shader 区域 ── */
.lo-left {
  flex: 0 0 55%; position: relative;
  background: #030812;
  display: flex; flex-direction: column;
  align-items: center; justify-content: center;
  overflow: hidden;
}
.lo-left-overlay {
  position: absolute; inset: 0;
  background: rgba(3,8,18,0.28);
  z-index: 1;
}
.lo-left-content {
  position: relative; z-index: 2;
  text-align: center; padding: 40px;
}
.lo-left-badge {
  display: inline-flex; align-items: center; gap: 8px;
  background: rgba(77,184,255,0.08);
  border: 1px solid rgba(77,184,255,0.22);
  color: rgba(145,205,255,0.9);
  font-family: 'DM Sans', sans-serif;
  font-size: 0.78rem; font-weight: 500;
  padding: 5px 16px; border-radius: 100px;
  letter-spacing: 0.05em; margin-bottom: 28px;
}
.lo-left-badge-dot {
  width: 6px; height: 6px; border-radius: 50%;
  background: #4db8ff; box-shadow: 0 0 8px #4db8ff;
  animation: loBadgePulse 2s infinite;
}
@keyframes loBadgePulse { 0%,100%{opacity:1}50%{opacity:.3} }
.lo-left-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: clamp(2rem, 4vw, 3.2rem);
  font-weight: 700; letter-spacing: 0.08em;
  color: #e8f4ff; line-height: 1.15; margin-bottom: 16px;
}
.lo-left-title em {
  font-style: normal;
  background: linear-gradient(100deg,#4db8ff 0%,#90d4ff 50%,#4db8ff 100%);
  background-size: 200% auto;
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  background-clip: text;
  animation: loShimmer 4s linear infinite;
}
@keyframes loShimmer { 0%{background-position:0% center}100%{background-position:200% center} }
.lo-left-sub {
  font-family: 'DM Sans', sans-serif;
  font-size: 0.92rem; color: rgba(160,200,240,0.55);
  line-height: 1.7; max-width: 340px; margin: 0 auto;
}

/* ── 右侧表单区域 ── */
.lo-right {
  flex: 0 0 45%;
  display: flex; align-items: center; justify-content: center;
  padding: 40px 56px;
  background: #030812;
  overflow-y: auto;
}

/* ── 表单卡片 ── */
.lo-card {
  width: 100%; max-width: 380px;
  background: rgba(5,13,30,0.85);
  border: 1px solid rgba(60,140,220,0.14);
  border-radius: 12px;
  padding: 36px 36px 32px;
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  display: flex; flex-direction: column; gap: 20px;
}

/* logo */
.lo-logo {
  text-align: center; margin-bottom: 4px;
}
.lo-logo-wordmark {
  font-family: 'Orbitron', sans-serif;
  font-size: 1.35rem; font-weight: 800;
  letter-spacing: 0.10em; color: #e8f4ff;
}
.lo-logo-wordmark em { color: #4db8ff; font-style: normal; }

/* 表单标题 */
.lo-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.7rem; font-weight: 700;
  color: #e8f4ff; text-align: center; margin: 0;
}

/* 表单组 */
.lo-group { display: flex; flex-direction: column; gap: 7px; }
.lo-group label {
  font-size: 0.875rem; font-weight: 500; color: #9ca3af;
  font-family: 'DM Sans', sans-serif;
}
.lo-group input {
  padding: 12px 14px;
  border: 1px solid rgba(60,140,220,0.2);
  border-radius: 6px;
  font-family: 'DM Sans', sans-serif;
  font-size: 1rem; color: #e8f4ff;
  background: rgba(3,8,18,0.6);
  transition: border-color 0.2s, box-shadow 0.2s;
  outline: none;
}
.lo-group input:focus {
  border-color: #4db8ff;
  box-shadow: 0 0 0 3px rgba(77,184,255,0.1);
}
.lo-group input::placeholder { color: #5a6073; }

/* options row */
.lo-options {
  display: flex; align-items: center;
  justify-content: space-between;
  font-size: 0.875rem; font-family: 'DM Sans', sans-serif;
}
.lo-remember {
  display: flex; align-items: center; gap: 6px;
  color: #9ca3af; cursor: pointer;
}
.lo-remember input[type="checkbox"] {
  width: 15px; height: 15px;
  accent-color: #4db8ff; cursor: pointer;
}
.lo-link {
  color: #4db8ff; cursor: pointer;
  text-decoration: none; transition: color 0.2s;
  background: none; border: none; font-size: 0.875rem;
  font-family: 'DM Sans', sans-serif; padding: 0;
}
.lo-link:hover { color: #90d4ff; text-decoration: underline; }

/* 主按钮 */
.lo-btn-primary {
  padding: 13px;
  background: #2962ff;
  color: #fff; border: none; border-radius: 6px;
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.05rem; font-weight: 600;
  cursor: pointer; transition: background 0.2s, transform 0.15s;
  letter-spacing: 0.05em;
}
.lo-btn-primary:hover:not(:disabled) { background: #1e53cc; transform: translateY(-1px); }
.lo-btn-primary:disabled { opacity: 0.65; cursor: not-allowed; }

/* 分隔线 */
.lo-divider {
  display: flex; align-items: center; gap: 14px;
  color: #6b7280; font-size: 0.875rem;
  font-family: 'DM Sans', sans-serif;
}
.lo-divider::before,.lo-divider::after {
  content: ''; flex: 1; height: 1px;
  background: rgba(60,140,220,0.2);
}

/* 社交按钮 */
.lo-social { display: flex; flex-direction: column; gap: 10px; }
.lo-social-btn {
  padding: 11px;
  border: 1px solid rgba(60,140,220,0.2);
  border-radius: 6px;
  background: rgba(3,8,18,0.6);
  color: #e8f4ff; cursor: pointer;
  display: flex; align-items: center; justify-content: center; gap: 8px;
  font-family: 'DM Sans', sans-serif; font-size: 0.95rem;
  transition: background 0.2s, border-color 0.2s;
}
.lo-social-btn:hover { background: rgba(77,184,255,0.1); border-color: #4db8ff; }
.lo-social-btn img { width: 18px; height: 18px; filter: brightness(0) invert(1); }

/* 切换链接 */
.lo-switch {
  text-align: center; font-size: 0.93rem;
  color: #9ca3af; font-family: 'DM Sans', sans-serif;
}

/* 错误信息 */
.lo-error {
  color: #f6465d; font-size: 0.88rem;
  padding: 8px 12px; background: rgba(246,70,93,0.08);
  border: 1px solid rgba(246,70,93,0.2);
  border-radius: 6px;
}

/* 成功信息 */
.lo-success {
  color: #0ecb81; font-size: 0.88rem;
  padding: 8px 12px; background: rgba(14,203,129,0.08);
  border: 1px solid rgba(14,203,129,0.2);
  border-radius: 6px; text-align: center;
}

/* 返回主页 */
.lo-back {
  text-align: center;
}
.lo-back button {
  background: none; border: none;
  color: rgba(255,255,255,0.38); font-size: 0.85rem;
  cursor: pointer; display: inline-flex; align-items: center; gap: 5px;
  font-family: 'DM Sans', sans-serif;
  transition: color 0.2s;
}
.lo-back button:hover { color: rgba(255,255,255,0.65); }

@media (max-width: 768px) {
  .lo-left { display: none; }
  .lo-right { flex: 1; padding: 24px 20px; }
}
`;function Om({onOpenLogin:e,onSuccess:t}){const[n,o]=x.useState("signin"),[i,a]=x.useState(""),[s,l]=x.useState(""),[c,d]=x.useState(""),[h,f]=x.useState(""),[g,y]=x.useState(!1),[v,b]=x.useState(""),[z,p]=x.useState(""),[u,m]=x.useState(!1);function j(){b(""),p("")}function T(P){o(P),b(""),p("")}async function w(P){if(P.preventDefault(),j(),!i||!c){b("请填写邮箱和密码");return}m(!0);try{const B=await(await fetch("/api/auth/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({email:i,pass:c})})).json();if(B.status==="error"){b(B.msg);return}if(B.token){localStorage.setItem("ikun_token",B.token);const K=B.user||{email:i,nickname:i.split("@")[0],role:"user"};localStorage.setItem("ikun_mock_user",JSON.stringify(K)),t==null||t(K)}}catch{if(i&&c){const N={email:i,nickname:i.split("@")[0],role:"user"};localStorage.setItem("ikun_token","mock-"+Date.now()),localStorage.setItem("ikun_mock_user",JSON.stringify(N)),t==null||t(N)}else b("网络错误，请重试")}finally{m(!1)}}async function C(P){if(P.preventDefault(),j(),!s||!i||!c){b("请填写所有字段");return}if(c!==h){b("两次密码不一致");return}if(c.length<6){b("密码至少 6 位");return}m(!0);try{const B=await(await fetch("/api/auth/register",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({username:s,email:i,pass:c})})).json();if(B.status==="error"){b(B.msg);return}if(B.token){localStorage.setItem("ikun_token",B.token);const K=B.user||{email:i,nickname:s,role:"user"};localStorage.setItem("ikun_mock_user",JSON.stringify(K)),t==null||t(K)}else p("注册成功！请登录"),setTimeout(()=>T("signin"),1200)}catch{const N={email:i,nickname:s,role:"user"};localStorage.setItem("ikun_token","mock-"+Date.now()),localStorage.setItem("ikun_mock_user",JSON.stringify(N)),t==null||t(N)}finally{m(!1)}}async function _(P){if(P.preventDefault(),j(),!i){b("请输入注册邮箱");return}m(!0);try{await fetch("/api/auth/forgot",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({email:i})}),p("重置链接已发送，请检查邮箱")}catch{b("发送失败，请重试")}finally{m(!1)}}return r.jsxs(r.Fragment,{children:[r.jsx("style",{children:Am}),r.jsxs("div",{className:"lo-wrap",children:[r.jsxs("div",{className:"lo-left",children:[r.jsx(Mm,{}),r.jsx("div",{className:"lo-left-overlay"})]}),r.jsx("div",{className:"lo-right",children:r.jsxs("div",{className:"lo-card",children:[r.jsx("div",{className:"lo-logo",children:r.jsxs("span",{className:"lo-logo-wordmark",children:[r.jsx("em",{children:"I"}),"KUNANCE"]})}),n==="signin"&&r.jsxs("form",{onSubmit:w,style:{display:"flex",flexDirection:"column",gap:16},children:[r.jsx("h2",{className:"lo-title",children:"Sign in"}),r.jsxs("div",{className:"lo-group",children:[r.jsx("label",{children:"Email or Username"}),r.jsx("input",{type:"text",placeholder:"Enter your email or username",value:i,onChange:P=>a(P.target.value),autoComplete:"username"})]}),r.jsxs("div",{className:"lo-group",children:[r.jsx("label",{children:"Password"}),r.jsx("input",{type:"password",placeholder:"Enter your password",value:c,onChange:P=>d(P.target.value),autoComplete:"current-password"})]}),r.jsxs("div",{className:"lo-options",children:[r.jsxs("label",{className:"lo-remember",children:[r.jsx("input",{type:"checkbox",checked:g,onChange:P=>y(P.target.checked)}),"Remember me"]}),r.jsx("button",{type:"button",className:"lo-link",onClick:()=>T("forgot"),children:"Forgot password?"})]}),v&&r.jsx("div",{className:"lo-error",children:v}),z&&r.jsx("div",{className:"lo-success",children:z}),r.jsx("button",{type:"submit",className:"lo-btn-primary",disabled:u,children:u?"Signing in...":"Sign in"}),r.jsx("div",{className:"lo-divider",children:"Or continue with"}),r.jsxs("div",{className:"lo-social",children:[r.jsxs("button",{type:"button",className:"lo-social-btn",children:[r.jsx("img",{src:"https://cdn-icons-png.flaticon.com/512/281/281764.png",alt:"Google"}),"Continue with Google"]}),r.jsxs("button",{type:"button",className:"lo-social-btn",children:[r.jsx("img",{src:"https://cdn-icons-png.flaticon.com/512/733/733579.png",alt:"X"}),"Continue with X"]})]}),r.jsxs("div",{className:"lo-switch",children:["Don't have an account?"," ",r.jsx("button",{type:"button",className:"lo-link",onClick:()=>T("signup"),children:"Sign up"})]})]}),n==="signup"&&r.jsxs("form",{onSubmit:C,style:{display:"flex",flexDirection:"column",gap:14},children:[r.jsx("h2",{className:"lo-title",children:"Sign up"}),r.jsxs("div",{className:"lo-group",children:[r.jsx("label",{children:"Username"}),r.jsx("input",{type:"text",placeholder:"Choose your username",value:s,onChange:P=>l(P.target.value)})]}),r.jsxs("div",{className:"lo-group",children:[r.jsx("label",{children:"Email"}),r.jsx("input",{type:"email",placeholder:"Enter your email",value:i,onChange:P=>a(P.target.value)})]}),r.jsxs("div",{className:"lo-group",children:[r.jsx("label",{children:"Password"}),r.jsx("input",{type:"password",placeholder:"Create your password (≥6 chars)",value:c,onChange:P=>d(P.target.value)})]}),r.jsxs("div",{className:"lo-group",children:[r.jsx("label",{children:"Confirm Password"}),r.jsx("input",{type:"password",placeholder:"Confirm your password",value:h,onChange:P=>f(P.target.value)})]}),v&&r.jsx("div",{className:"lo-error",children:v}),z&&r.jsx("div",{className:"lo-success",children:z}),r.jsx("button",{type:"submit",className:"lo-btn-primary",disabled:u,children:u?"Creating account...":"Sign up"}),r.jsx("div",{className:"lo-divider",children:"Or continue with"}),r.jsxs("div",{className:"lo-social",children:[r.jsxs("button",{type:"button",className:"lo-social-btn",children:[r.jsx("img",{src:"https://cdn-icons-png.flaticon.com/512/281/281764.png",alt:"Google"}),"Continue with Google"]}),r.jsxs("button",{type:"button",className:"lo-social-btn",children:[r.jsx("img",{src:"https://cdn-icons-png.flaticon.com/512/733/733579.png",alt:"X"}),"Continue with X"]})]}),r.jsxs("div",{className:"lo-switch",children:["Already have an account?"," ",r.jsx("button",{type:"button",className:"lo-link",onClick:()=>T("signin"),children:"Sign in"})]})]}),n==="forgot"&&r.jsxs("form",{onSubmit:_,style:{display:"flex",flexDirection:"column",gap:16},children:[r.jsx("h2",{className:"lo-title",children:"Reset Password"}),r.jsxs("div",{className:"lo-group",children:[r.jsx("label",{children:"Email"}),r.jsx("input",{type:"email",placeholder:"Enter your registered email",value:i,onChange:P=>a(P.target.value)})]}),r.jsx("p",{style:{fontFamily:"'DM Sans', sans-serif",fontSize:"0.85rem",color:"#9ca3af",margin:0},children:"We'll send a password reset link to your email address."}),v&&r.jsx("div",{className:"lo-error",children:v}),z&&r.jsx("div",{className:"lo-success",children:z}),r.jsx("button",{type:"submit",className:"lo-btn-primary",disabled:u,children:u?"Sending...":"Send Reset Link"}),r.jsx("div",{className:"lo-switch",children:r.jsx("button",{type:"button",className:"lo-link",onClick:()=>T("signin"),children:"← Back to Sign in"})})]}),r.jsx("div",{className:"lo-back",children:r.jsxs("button",{onClick:e,children:[r.jsx("svg",{width:"13",height:"13",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",children:r.jsx("path",{d:"M19 12H5M12 19l-7-7 7-7"})}),"Back to Home"]})})]})})]})]})}const ec=[{id:"monitor",label:"监控",active:!0},{id:"signals",label:"信号",active:!0},{id:"market",label:"市场",active:!0,hasDropdown:!0},{id:"community",label:"社区",active:!0},{id:"indicators",label:"指标",active:!0},{id:"premium",label:"会员",active:!0}],tc=[{name:"加密货币",items:[{id:"binance",label:"Binance",desc:"币安合约"},{id:"okx",label:"OKX",desc:"欧易合约"},{id:"bitget",label:"Bitget",desc:"Bitget 合约"},{id:"bybit",label:"Bybit",desc:"Bybit 合约"}]},{name:"传统市场",items:[{id:"us",label:"美股",desc:"NYSE / NASDAQ"},{id:"hk",label:"港股",desc:"HKEX"}]}],nc=[{id:"dark",label:"Dark",icon:"🌙"},{id:"light",label:"Light",icon:"☀️"},{id:"vc",label:"罪城",icon:"🌆"},{id:"mc",label:"MC",icon:"⛏️"}],oo={dark:{"--bg-color":"#030812","--bg-secondary":"#0b0e11","--sidebar-bg":"#0b0e11","--card-bg":"rgba(5,13,30,0.85)","--text-primary":"#e2eaf4","--text-secondary":"#9ca3af","--border-color":"rgba(60,140,220,0.14)","--nav-bg":"rgba(4,10,22,0.92)","--nav-border":"rgba(60,140,220,0.14)","--accent":"#4db8ff","--ikun-blue":"#4db8ff","--binance-yellow":"#4db8ff","--hover-bg":"rgba(77,184,255,0.08)","--input-bg":"rgba(5,13,30,0.6)"},light:{"--bg-color":"#f0f4fa","--bg-secondary":"#e4eaf5","--sidebar-bg":"#ffffff","--card-bg":"rgba(255,255,255,0.92)","--text-primary":"#0d1a2e","--text-secondary":"#4a5568","--border-color":"rgba(40,100,180,0.18)","--nav-bg":"rgba(240,244,250,0.96)","--nav-border":"rgba(40,100,180,0.18)","--accent":"#1a56db","--ikun-blue":"#1a56db","--binance-yellow":"#1a56db","--hover-bg":"rgba(26,86,219,0.08)","--input-bg":"#ffffff"},vc:{"--bg-color":"#2a2535","--bg-secondary":"#231f2e","--sidebar-bg":"#201c2b","--card-bg":"rgba(45,38,58,0.95)","--text-primary":"#ff79c6","--text-secondary":"#bd93f9","--border-color":"rgba(80,255,180,0.35)","--nav-bg":"rgba(32,28,43,0.96)","--nav-border":"rgba(80,255,180,0.3)","--accent":"#ff79c6","--ikun-blue":"#50ffb4","--binance-yellow":"#ff79c6","--binance-green":"#50ffb4","--binance-red":"#ff5555","--hover-bg":"rgba(255,121,198,0.1)","--input-bg":"rgba(35,31,46,0.9)"},mc:{"--bg-color":"#1a1209","--bg-secondary":"#221808","--sidebar-bg":"#221808","--card-bg":"rgba(30,20,8,0.92)","--text-primary":"#f5dfa0","--text-secondary":"#a08858","--border-color":"rgba(180,120,20,0.22)","--nav-bg":"rgba(20,14,4,0.96)","--nav-border":"rgba(180,120,20,0.22)","--accent":"#e0a020","--ikun-blue":"#e0a020","--binance-yellow":"#7CCC19","--hover-bg":"rgba(224,160,32,0.1)","--input-bg":"rgba(30,20,8,0.8)"}};function sn({activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}){var j,T;const[a,s]=x.useState(!1),[l,c]=x.useState(!1),[d,h]=x.useState(!1),[f,g]=x.useState(!1),[y,v]=x.useState(()=>localStorage.getItem("ikun_theme")||"dark");x.useEffect(()=>{const w=localStorage.getItem("ikun_theme")||"dark",C=document.documentElement;C.setAttribute("data-theme",w);const _=oo[w]||oo.dark;Object.entries(_).forEach(([P,N])=>C.style.setProperty(P,N))},[]);const b=x.useRef(null),z=x.useRef(null),p=x.useRef(null);x.useEffect(()=>{function w(C){b.current&&!b.current.contains(C.target)&&s(!1),z.current&&!z.current.contains(C.target)&&c(!1),p.current&&!p.current.contains(C.target)&&h(!1)}return document.addEventListener("mousedown",w),()=>document.removeEventListener("mousedown",w)},[]);function u(w){if(w.hasDropdown){s(C=>!C);return}g(!1),t(w.id)}function m(w){v(w.id),c(!1),localStorage.setItem("ikun_theme",w.id);const C=document.documentElement;C.setAttribute("data-theme",w.id);const _=oo[w.id]||oo.dark;Object.entries(_).forEach(([P,N])=>C.style.setProperty(P,N))}return r.jsxs(r.Fragment,{children:[r.jsx("style",{children:Bm}),r.jsxs("nav",{className:"gn-nav",children:[r.jsxs("button",{className:"gn-logo",onClick:()=>t("monitor"),children:[r.jsx("img",{className:"gn-logo-img gn-logo-dark",src:"https://sc01.alicdn.com/kf/Ac3903ff596f74d1f804452cc4ffff11e4.png",alt:"IK"}),r.jsx("img",{className:"gn-logo-img gn-logo-light",src:"https://sc01.alicdn.com/kf/A62ded46173cd4a2ba0fe462ba9fdfc1eM.png",alt:"IK"}),r.jsxs("span",{className:"gn-logo-word",children:[r.jsx("em",{children:"I"}),"KUNANCE"]})]}),r.jsx("div",{className:"gn-items",children:ec.map(w=>r.jsxs("div",{ref:w.hasDropdown?b:null,style:{position:"relative"},children:[r.jsxs("button",{className:`gn-btn ${e===w.id?"gn-btn--active":""} ${w.id==="premium"?"gn-btn--premium":""}`,onClick:()=>u(w),children:[w.id==="premium"?r.jsx("span",{className:"gn-premium-label",children:w.label}):w.label,w.hasDropdown&&r.jsx("svg",{className:`gn-caret ${a?"gn-caret--open":""}`,viewBox:"0 0 10 6",fill:"none",width:"9",children:r.jsx("path",{d:"M1 1l4 4 4-4",stroke:"currentColor",strokeWidth:"1.6",strokeLinecap:"round"})})]}),w.hasDropdown&&a&&r.jsx("div",{className:"gn-dropdown",children:tc.map(C=>r.jsxs("div",{className:"gn-dd-group",children:[r.jsx("div",{className:"gn-dd-grouplabel",children:C.name}),C.items.map(_=>r.jsxs("button",{className:"gn-dd-item",onClick:()=>{s(!1),t("market",_.id)},children:[r.jsx("span",{className:"gn-dd-name",children:_.label}),r.jsx("span",{className:"gn-dd-desc",children:_.desc})]},_.id))]},C.name))})]},w.id))}),r.jsxs("div",{className:"gn-right",children:[r.jsxs("div",{ref:z,style:{position:"relative"},children:[r.jsxs("button",{className:"gn-hub-btn",onClick:()=>c(w=>!w),title:"主题",children:[r.jsx("svg",{viewBox:"0 0 20 20",fill:"none",width:"16",height:"16",children:y==="light"?r.jsxs(r.Fragment,{children:[r.jsx("circle",{cx:"10",cy:"10",r:"4",stroke:"currentColor",strokeWidth:"1.4"}),r.jsx("path",{d:"M10 2v2M10 16v2M2 10h2M16 10h2M4.5 4.5l1.5 1.5M14 14l1.5 1.5M4.5 15.5L6 14M14 6l1.5-1.5",stroke:"currentColor",strokeWidth:"1.4",strokeLinecap:"round"})]}):r.jsx("path",{d:"M17.5 11.5A7.5 7.5 0 119.5 2.5a5.5 5.5 0 008 9z",stroke:"currentColor",strokeWidth:"1.4",strokeLinejoin:"round"})}),r.jsx("span",{children:((j=nc.find(w=>w.id===y))==null?void 0:j.label)||"Dark"})]}),l&&r.jsx("div",{className:"gn-dropdown gn-dropdown--right",children:nc.map(w=>r.jsx("button",{className:`gn-dd-item ${y===w.id?"gn-dd-item--sel":""}`,onClick:()=>m(w),children:w.label},w.id))})]}),r.jsxs("button",{className:"gn-hub-btn",onClick:()=>t("settings"),title:"设置",children:[r.jsxs("svg",{viewBox:"0 0 20 20",fill:"none",width:"16",height:"16",children:[r.jsx("circle",{cx:"10",cy:"10",r:"3",stroke:"currentColor",strokeWidth:"1.4"}),r.jsx("path",{d:"M10 2v1.5M10 16.5V18M2 10h1.5M16.5 10H18M4.1 4.1l1.1 1.1M14.8 14.8l1.1 1.1M4.1 15.9l1.1-1.1M14.8 5.2l1.1-1.1",stroke:"currentColor",strokeWidth:"1.4",strokeLinecap:"round"})]}),r.jsx("span",{children:"设置"})]}),n?r.jsxs("div",{ref:p,style:{position:"relative"},children:[r.jsxs("button",{className:"gn-user-btn",onClick:()=>h(w=>!w),children:[n.avatar?r.jsx("img",{src:n.avatar,alt:"",className:"gn-avatar"}):r.jsx("span",{className:"gn-avatar-text",children:(n.nickname||n.email||"?")[0].toUpperCase()}),r.jsx("span",{className:"gn-user-name",children:n.nickname||((T=n.email)==null?void 0:T.split("@")[0])}),r.jsx("svg",{className:`gn-caret ${d?"gn-caret--open":""}`,viewBox:"0 0 10 6",fill:"none",width:"9",children:r.jsx("path",{d:"M1 1l4 4 4-4",stroke:"currentColor",strokeWidth:"1.6",strokeLinecap:"round"})})]}),d&&r.jsxs("div",{className:"gn-dropdown gn-dropdown--right",children:[r.jsxs("div",{className:"gn-dd-userinfo",children:[r.jsx("div",{className:"gn-dd-username",children:n.nickname}),r.jsx("div",{className:"gn-dd-email",children:n.email})]}),r.jsx("button",{className:"gn-dd-item",onClick:()=>{h(!1),t("settings")},children:"账户设置"}),r.jsx("button",{className:"gn-dd-item gn-dd-item--danger",onClick:()=>{h(!1),i==null||i()},children:"退出登录"})]})]}):r.jsxs("button",{className:"gn-login-btn",onClick:o,children:[r.jsxs("svg",{viewBox:"0 0 20 20",fill:"none",width:"15",height:"15",children:[r.jsx("circle",{cx:"10",cy:"7",r:"3.5",stroke:"currentColor",strokeWidth:"1.4"}),r.jsx("path",{d:"M3 18c0-3.3 3.1-6 7-6s7 2.7 7 6",stroke:"currentColor",strokeWidth:"1.4",strokeLinecap:"round"})]}),"登录"]})]}),r.jsxs("button",{className:`gn-burger ${f?"gn-burger--open":""}`,onClick:()=>g(w=>!w),"aria-label":"菜单",children:[r.jsx("span",{}),r.jsx("span",{}),r.jsx("span",{})]})]}),f&&r.jsx("div",{className:"gn-mobile-overlay",onClick:()=>g(!1),children:r.jsxs("div",{className:"gn-mobile-drawer",onClick:w=>w.stopPropagation(),children:[ec.map(w=>r.jsxs("div",{children:[r.jsxs("button",{className:`gn-mobile-item ${e===w.id?"gn-mobile-item--active":""} ${w.id==="premium"?"gn-mobile-item--premium":""}`,onClick:()=>{if(w.hasDropdown){s(C=>!C);return}g(!1),t(w.id)},children:[w.id==="premium"?r.jsx("span",{className:"gn-premium-label",children:w.label}):w.label,w.hasDropdown&&r.jsx("svg",{className:`gn-caret ${a?"gn-caret--open":""}`,viewBox:"0 0 10 6",fill:"none",width:"10",style:{marginLeft:"auto"},children:r.jsx("path",{d:"M1 1l4 4 4-4",stroke:"currentColor",strokeWidth:"1.6",strokeLinecap:"round"})})]}),w.hasDropdown&&a&&r.jsx("div",{className:"gn-mobile-sub",children:tc.map(C=>r.jsxs("div",{children:[r.jsx("div",{className:"gn-dd-grouplabel",children:C.name}),C.items.map(_=>r.jsxs("button",{className:"gn-mobile-subitem",onClick:()=>{g(!1),s(!1),t("market",_.id)},children:[r.jsx("span",{className:"gn-dd-name",children:_.label}),r.jsx("span",{className:"gn-dd-desc",children:_.desc})]},_.id))]},C.name))})]},w.id)),r.jsx("div",{className:"gn-mobile-divider"}),n?r.jsx("button",{className:"gn-mobile-item gn-mobile-item--danger",onClick:()=>{g(!1),i&&i()},children:"退出登录"}):r.jsx("button",{className:"gn-mobile-item",onClick:()=>{g(!1),o&&o()},children:"登录 / 注册"})]})})]})}const Bm=`
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=Orbitron:wght@700;800&display=swap');

.gn-nav {
  position: sticky; top: 0; z-index: 200;
  display: flex; align-items: center;
  height: 56px; padding: 0 28px;
  background: var(--nav-bg, rgba(4,10,22,0.92));
  border-bottom: 1px solid var(--nav-border, rgba(60,140,220,0.14));
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  flex-shrink: 0;
  gap: 0;
  transition: background 0.3s, border-color 0.3s;
}

/* Logo */
.gn-logo {
  display: flex; align-items: center; gap: 10px;
  background: none; border: none; cursor: pointer;
  margin-right: 36px; flex-shrink: 0;
  padding: 0;
}
.gn-logo-img { width: 32px; height: 32px; }

/* 暗色 Logo：默认显示（screen 混合让白色笔画穿透深色背景） */
.gn-logo-dark {
  mix-blend-mode: screen;
  opacity: 0.90;
  display: block;
}
/* 浅色 Logo：默认隐藏 */
.gn-logo-light { display: none; }

/* light 主题：切换两者可见性 */
[data-theme="light"] .gn-logo-dark  { display: none; }
[data-theme="light"] .gn-logo-light { display: block; mix-blend-mode: normal; opacity: 1; }
.gn-logo-word {
  font-family: 'Orbitron', sans-serif;
  font-size: 1.05rem; font-weight: 800;
  letter-spacing: .10em; color: var(--text-primary);
  line-height: 1;
}
.gn-logo-word em { color: var(--accent); font-style: normal; }

/* Nav items */
.gn-items {
  display: flex; align-items: center; gap: 0; flex: 1;
}
.gn-btn {
  background: none; border: none;
  font-family: 'Rajdhani', sans-serif;
  font-size: 1rem; font-weight: 600;
  letter-spacing: .06em;
  color: var(--text-secondary);
  padding: 7px 16px; border-radius: 6px;
  cursor: pointer;
  display: flex; align-items: center; gap: 5px;
  position: relative; transition: color .18s;
  white-space: nowrap;
}
.gn-btn::after {
  content: ''; position: absolute;
  bottom: 4px; left: 50%; transform: translateX(-50%);
  width: 0; height: 2px;
  background: var(--accent); border-radius: 2px;
  transition: width .22s ease;
}
.gn-btn:hover { color: var(--text-primary); }
.gn-btn:hover::after { width: 50%; }
.gn-btn--active { color: var(--accent); }
.gn-btn--active::after { width: 50%; }

.gn-caret { transition: transform .2s; flex-shrink: 0; opacity: .55; }
.gn-caret--open { transform: rotate(180deg); }

/* ── 会员按钮炫彩发光 ── */
@keyframes gn-premium-shimmer {
  0%   { background-position: 0% 50% }
  50%  { background-position: 100% 50% }
  100% { background-position: 0% 50% }
}
@keyframes gn-premium-txt-glow {
  0%,100% { text-shadow: 0 0 8px rgba(77,184,255,0.9), 0 0 16px rgba(41,98,255,0.6); }
  33%     { text-shadow: 0 0 8px rgba(160,64,255,0.9), 0 0 16px rgba(120,40,220,0.6); }
  66%     { text-shadow: 0 0 8px rgba(0,200,255,0.9), 0 0 16px rgba(0,140,255,0.6); }
}
/* 按钮本身完全透明，不产生任何方块 */
.gn-btn--premium {
  background: none !important;
  box-shadow: none !important;
}
/* ::after 横线完全禁用 */
.gn-btn--premium::after,
.gn-btn--active.gn-btn--premium::after {
  display: none !important;
}
/* 文字渐变发光放在 span 上 */
.gn-btn--premium .gn-premium-label {
  background: linear-gradient(90deg, #4db8ff, #2962ff, #a040ff, #00d4ff, #4db8ff);
  background-size: 300% auto;
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  animation: gn-premium-shimmer 4s linear infinite, gn-premium-txt-glow 3s ease-in-out infinite;
}

/* Right Hub */
.gn-right {
  display: flex; align-items: center; gap: 4px;
  margin-left: auto; flex-shrink: 0;
}
.gn-hub-btn {
  background: none; border: none;
  color: var(--text-secondary);
  font-family: 'Rajdhani', sans-serif;
  font-size: .92rem; font-weight: 600;
  letter-spacing: .04em;
  padding: 6px 12px; border-radius: 6px;
  cursor: pointer;
  display: flex; align-items: center; gap: 6px;
  transition: background .18s, color .18s;
}
.gn-hub-btn:hover {
  background: var(--hover-bg);
  color: var(--text-primary);
}
.gn-hub-btn svg { flex-shrink: 0; }

/* Login button */
.gn-login-btn {
  display: flex; align-items: center; gap: 7px;
  background: #082d6e;
  border: 1px solid rgba(77, 184, 255, .36);
  color: #e8f4ff;
  font-family: 'Rajdhani', sans-serif;
  font-size: .98rem; font-weight: 700;
  letter-spacing: .06em;
  padding: 7px 18px; border-radius: 7px;
  cursor: pointer;
  transition: background .2s, box-shadow .2s, transform .15s;
  margin-left: 6px;
}
.gn-login-btn:hover {
  background: #0d3d8f;
  box-shadow: 0 0 16px rgba(77, 184, 255, .3);
  transform: translateY(-1px);
}

/* User button */
.gn-user-btn {
  display: flex; align-items: center; gap: 8px;
  background: var(--hover-bg);
  border: 1px solid var(--border-color);
  color: var(--text-primary);
  font-family: 'Rajdhani', sans-serif;
  font-size: .95rem; font-weight: 600;
  padding: 6px 12px; border-radius: 7px;
  cursor: pointer; transition: background .18s;
  margin-left: 6px;
}
.gn-user-btn:hover { background: color-mix(in srgb, var(--accent) 15%, transparent); }
.gn-avatar {
  width: 24px; height: 24px; border-radius: 50%;
  object-fit: cover;
}
.gn-avatar-text {
  width: 24px; height: 24px; border-radius: 50%;
  background: color-mix(in srgb, var(--accent) 25%, transparent);
  display: flex; align-items: center; justify-content: center;
  font-size: .78rem; font-weight: 700; color: var(--accent);
}
.gn-user-name { max-width: 100px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* Dropdown — 全部用 CSS 变量跟随主题 */
.gn-dropdown {
  position: absolute; top: calc(100% + 8px); left: 0;
  min-width: 190px;
  background: var(--nav-bg, rgba(6,14,32,0.96));
  border: 1px solid var(--nav-border, rgba(48,120,200,.22));
  border-radius: 10px; padding: 8px;
  backdrop-filter: blur(24px);
  box-shadow: 0 20px 60px rgba(0,0,0,.45);
  animation: gnDropIn .16s ease;
  z-index: 300;
}
.gn-dropdown--right { left: auto; right: 0; }
@keyframes gnDropIn {
  from { opacity: 0; transform: translateY(-6px); }
  to   { opacity: 1; transform: translateY(0); }
}
.gn-dd-group { margin-bottom: 6px; }
.gn-dd-group:last-child { margin-bottom: 0; }
.gn-dd-grouplabel {
  font-family: 'Rajdhani', sans-serif;
  font-size: .72rem; font-weight: 700;
  color: var(--accent, #4db8ff);
  opacity: 0.6;
  letter-spacing: .08em; text-transform: uppercase;
  padding: 4px 10px 5px;
}
.gn-dd-item {
  width: 100%; background: none; border: none;
  color: var(--text-primary, rgba(200,220,240,.85));
  font-family: 'Rajdhani', sans-serif;
  font-size: .92rem; font-weight: 600;
  padding: 8px 10px; border-radius: 6px;
  cursor: pointer;
  display: flex; flex-direction: column; gap: 1px;
  text-align: left;
  transition: background .15s, color .15s;
}
.gn-dd-item:hover { background: var(--hover-bg, rgba(77,184,255,.12)); color: var(--text-primary); }
.gn-dd-item--sel { color: var(--accent, #4db8ff); }
.gn-dd-item--danger { color: rgba(246,70,93,.8); }
.gn-dd-item--danger:hover { background: rgba(246,70,93,.10); color: #f6465d; }
.gn-dd-name { font-size: .9rem; }
.gn-dd-desc { font-size: .74rem; opacity: .5; font-weight: 500; }

.gn-dd-userinfo {
  padding: 10px 10px 8px;
  border-bottom: 1px solid var(--border-color, rgba(60,120,200,.15));
  margin-bottom: 4px;
}
.gn-dd-username { font-size: .9rem; font-weight: 700; color: var(--text-primary, #c8e6ff); }
.gn-dd-email { font-size: .75rem; color: var(--text-secondary); opacity: .7; margin-top: 2px; }

/* ── 移动端响应式 ── */
@media (max-width: 768px) {
  .gn-items { display: none; }
  .gn-hub-btn span { display: none; }
  .gn-nav { padding: 0 16px; }
  .gn-logo { margin-right: auto; }
}

/* 汉堡按钮 */
.gn-burger {
  display: none;
  flex-direction: column; justify-content: center; align-items: center;
  gap: 5px;
  width: 36px; height: 36px;
  background: none; border: none; cursor: pointer;
  padding: 4px; border-radius: 6px;
  flex-shrink: 0; margin-left: 8px;
  transition: background .18s;
}
.gn-burger:hover { background: var(--hover-bg); }
.gn-burger span {
  display: block;
  width: 20px; height: 2px;
  background: var(--text-secondary);
  border-radius: 2px;
  transition: transform .22s ease, opacity .18s;
  transform-origin: center;
}
.gn-burger--open span:nth-child(1) { transform: translateY(7px) rotate(45deg); background: var(--accent); }
.gn-burger--open span:nth-child(2) { opacity: 0; transform: scaleX(0); }
.gn-burger--open span:nth-child(3) { transform: translateY(-7px) rotate(-45deg); background: var(--accent); }
@media (max-width: 768px) { .gn-burger { display: flex; } }

/* 移动端抽屉覆盖 */
.gn-mobile-overlay {
  position: fixed; inset: 0; z-index: 199;
  background: rgba(0,0,0,0.45);
  backdrop-filter: blur(4px);
  animation: gnFadeIn .18s ease;
}
@keyframes gnFadeIn { from { opacity: 0; } to { opacity: 1; } }
.gn-mobile-drawer {
  position: absolute; top: 56px; left: 0; right: 0;
  background: var(--nav-bg, rgba(4,10,22,0.98));
  border-bottom: 1px solid var(--nav-border, rgba(60,140,220,0.18));
  padding: 8px 0 16px;
  backdrop-filter: blur(24px);
  animation: gnSlideDown .2s ease;
}
@keyframes gnSlideDown {
  from { opacity: 0; transform: translateY(-8px); }
  to   { opacity: 1; transform: translateY(0); }
}
.gn-mobile-item {
  width: 100%; background: none; border: none;
  display: flex; align-items: center; gap: 10px;
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.05rem; font-weight: 600;
  letter-spacing: .05em;
  color: var(--text-secondary);
  padding: 13px 24px;
  cursor: pointer; text-align: left;
  transition: color .15s, background .15s;
}
.gn-mobile-item:hover { color: var(--text-primary); background: var(--hover-bg); }
.gn-mobile-item--active { color: var(--accent); }
.gn-mobile-item--premium .gn-premium-label { -webkit-text-fill-color: unset; background: none; color: var(--accent); }
.gn-mobile-item--danger { color: rgba(246,70,93,.75); }
.gn-mobile-item--danger:hover { color: #f6465d; background: rgba(246,70,93,.08); }
.gn-mobile-sub { padding: 0 12px; }
.gn-mobile-subitem {
  width: 100%; background: none; border: none;
  display: flex; flex-direction: column; gap: 2px;
  font-family: 'Rajdhani', sans-serif;
  color: var(--text-secondary);
  padding: 10px 16px; border-radius: 7px;
  cursor: pointer; text-align: left;
  transition: background .15s, color .15s;
}
.gn-mobile-subitem:hover { background: var(--hover-bg); color: var(--text-primary); }
.gn-mobile-divider { height: 1px; background: var(--border-color); margin: 8px 16px; }
`;let Bi=null;function Fm(){return Bi||(Bi=new(window.AudioContext||window.webkitAudioContext)),Bi}function pn(e,t,n){const o=Fm();o.state==="suspended"&&o.resume();const i=o.createOscillator(),a=o.createGain();i.connect(a),a.connect(o.destination),i.type=t,i.frequency.setValueAtTime(e,o.currentTime),a.gain.setValueAtTime(.1,o.currentTime),i.start(),i.stop(o.currentTime+n)}function Cu(e){if(e&&e.startsWith("custom:")){const t=new Audio("/api/sound/"+e.replace("custom:",""));t.volume=.5,t.play();return}e==="retro"?(pn(600,"square",.1),setTimeout(()=>pn(800,"square",.1),100)):e==="alarm"?(pn(1e3,"sawtooth",.2),setTimeout(()=>pn(1e3,"sawtooth",.2),300)):e==="sonar"?pn(300,"sine",.5):pn(440,"sine",.2)}function Um({sidebarOpen:e,timeframe:t,setTimeframe:n,triggerMode:o,setTriggerMode:i,watchlistMode:a,setWatchlistMode:s,alertSettings:l,syncAlertSettings:c,setShowEmailBindTip:d,connStatus:h,customSounds:f,onOpenSettings:g,setShowStratModal:y}){return r.jsxs("div",{className:`sidebar ${e?"open":""}`,children:[r.jsx("h2",{style:{color:"var(--text-primary)",marginTop:0,fontSize:16,marginBottom:20,borderLeft:"3px solid var(--accent)",paddingLeft:10},children:"指标配置"}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"当前指标"}),r.jsxs("select",{className:"bn-select",children:[r.jsx("option",{children:"MACD 趋势确认/演进"}),(()=>{try{return JSON.parse(localStorage.getItem("ikun_saved_indicators")||"[]").map(b=>r.jsx("option",{children:b.name},b.id))}catch{return null}})(),r.jsx("option",{disabled:!0,children:"─────────────"}),r.jsx("option",{disabled:!0,children:"RSI 超买超卖 (即将上线)"}),r.jsx("option",{disabled:!0,children:"布林带突破 (即将上线)"})]}),r.jsx("button",{onClick:()=>y(!0),style:{marginTop:8,width:"100%",padding:8,background:"var(--input-bg)",border:"1px dashed var(--border-color)",color:"var(--text-secondary)",borderRadius:4,cursor:"pointer",fontSize:12},children:"+ 添加更多指标"})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"监控周期 (Timeframe)"}),r.jsxs("select",{className:"bn-select",value:t,onChange:v=>c({...l,_timeframe:v.target.value},v.target.value),children:[r.jsx("option",{value:"15m",children:"15m"}),r.jsx("option",{value:"1h",children:"1h"}),r.jsx("option",{value:"4h",children:"4h"}),r.jsx("option",{value:"1d",children:"1d"})]})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"触发条件 (Trigger)"}),r.jsxs("select",{className:"bn-select",value:o,onChange:v=>{i(v.target.value),c(l,t,v.target.value)},children:[r.jsx("option",{value:"close",children:"每根K线收盘时触发一次"}),r.jsx("option",{value:"every",children:"每根K线触发一次"})]})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"监控列表模式"}),r.jsxs("select",{className:"bn-select",value:a,onChange:v=>{s(v.target.value),c(l,t,o,v.target.value)},children:[r.jsx("option",{value:"favorites",children:"自选列表 (Favorites)"}),r.jsx("option",{value:"movers",children:"今日涨跌榜 (Market Movers)"})]})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"警报渠道 (快速设置)"}),r.jsx("div",{className:"checkbox-group",children:[["toast","网页弹窗"],["sound","声音提醒"],["app_push","APP 推送"],["email","邮件发送"]].map(([v,b])=>r.jsxs("label",{className:"checkbox-item",children:[r.jsx("input",{type:"checkbox",checked:!!l[v],onChange:z=>{const p={...l,[v]:z.target.checked};c(p),v==="email"&&z.target.checked&&d(!0)}}),b]},v))}),r.jsxs("div",{style:{marginTop:15},children:[r.jsxs("span",{className:"label",children:[r.jsxs("svg",{width:"14",height:"14",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{marginRight:6,verticalAlign:-2},children:[r.jsx("path",{d:"M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"}),r.jsx("path",{d:"M13.73 21a2 2 0 0 1-3.46 0"})]}),"提示音效 (Ringtone)"]}),r.jsxs("select",{className:"bn-select",value:l.sound_type||"beep",onChange:v=>{const b={...l,sound_type:v.target.value};c(b),Cu(v.target.value)},children:[r.jsx("option",{value:"beep",children:"标准 Beep (默认)"}),r.jsx("option",{value:"retro",children:"复古游戏 (Retro)"}),r.jsx("option",{value:"alarm",children:"紧急警报 (Alarm)"}),r.jsx("option",{value:"sonar",children:"深海声纳 (Sonar)"}),f.map(v=>r.jsx("option",{value:"custom:"+v.file,children:v.name},v.file))]})]})]}),r.jsxs("div",{className:"status-panel-large",children:[r.jsx("div",{className:"status-pulse"}),r.jsx("div",{style:{color:"var(--binance-green)",fontWeight:"bold",fontSize:15,marginBottom:5},children:"全自动监控运行中"}),r.jsxs("div",{style:{color:"var(--text-secondary)",fontSize:12},children:["状态: ",h,r.jsx("br",{}),"监控中"]})]})]})}function Wm({trend:e}){return e==="BULL"?r.jsxs("span",{className:"trend-bull",children:[r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{marginRight:4,verticalAlign:-2},children:[r.jsx("polyline",{points:"23 6 13.5 15.5 8.5 10.5 1 18"}),r.jsx("polyline",{points:"17 6 23 6 23 12"})]}),"多头 Bull"]}):r.jsxs("span",{className:"trend-bear",children:[r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{marginRight:4,verticalAlign:-2},children:[r.jsx("polyline",{points:"23 18 13.5 8.5 8.5 13.5 1 6"}),r.jsx("polyline",{points:"17 18 23 18 23 12"})]}),"空头 Bear"]})}function $m({signal:e,action:t}){return e==="-"?r.jsx("span",{style:{color:"#555"},children:"-"}):t==="LONG"?r.jsx("span",{className:"signal-badge badge-yellow",children:e}):r.jsx("span",{className:"signal-badge badge-purple",children:e})}function Tu({wrapRef:e,value:t,onChange:n,onFocus:o,onKeyDown:i,showDropdown:a,filteredSymbols:s,inListFn:l,onAdd:c}){return r.jsxs("div",{className:"main-search-container",ref:e,children:[r.jsx("input",{className:"main-search-input",placeholder:"搜索永续合约标的 (输入: BTC, ETH, SOL...)",value:t,onFocus:o,onChange:n,onKeyDown:i}),r.jsx("span",{className:"main-search-icon",children:r.jsxs("svg",{width:"20",height:"20",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",children:[r.jsx("circle",{cx:"11",cy:"11",r:"8"}),r.jsx("line",{x1:"21",y1:"21",x2:"16.65",y2:"16.65"})]})}),a&&t&&r.jsx("div",{className:"search-dropdown",children:s.length===0?r.jsx("div",{className:"search-dd-item",style:{color:"var(--text-secondary)"},children:"未找到匹配的标的"}):s.map(d=>{const h=l(d.key);return r.jsxs("div",{className:"search-dd-item",children:[r.jsxs("div",{className:"dd-item-main",children:[r.jsx("span",{className:"dd-sym-name",children:d.display}),r.jsxs("span",{className:"dd-sym-meta",children:[r.jsx("span",{className:"dd-tag dd-exchange",children:d.exchangeLabel}),r.jsx("span",{className:"dd-tag dd-type",children:"合约"})]})]}),h?r.jsx("span",{className:"in-list",children:"已添加"}):r.jsx("button",{className:"add-btn",onClick:f=>{f.stopPropagation(),c(d)},children:"+ 添加"})]},d.key)})})]})}function IKTVS(e){const t={binance:"BINANCE",okx:"OKX",bybit:"BYBIT",bitget:"BITGET",gate:"GATEIO",kucoin:"KUCOIN"}[e.exchangeId]||"BINANCE",n=String(e.symbol||"").toUpperCase().replace("/","").replace(/[^A-Z0-9]/g,"");return`${t}:${n}.P`}function IKTVB({wlItem:e}){const t=IKTVS(e),n=`ikun_tv_${t.replace(/[^A-Z0-9]/gi,"_")}`,o=`https://s.tradingview.com/widgetembed/?frameElementId=${n}&symbol=${encodeURIComponent(t)}&interval=15&hidesidetoolbar=1&symboledit=1&saveimage=0&toolbarbg=151923&studies=[]&theme=dark&style=1&timezone=Asia%2FShanghai&withdateranges=1`;return r.jsx("div",{style:{width:"100%",height:260,border:"1px solid var(--border-color)",borderRadius:8,overflow:"hidden",background:"var(--bg-color)"},children:r.jsx("iframe",{id:n,title:`${e.symbol} TradingView`,src:o,loading:"lazy",referrerPolicy:"origin",style:{width:"100%",height:"100%",border:0,display:"block"}})})}function IKTVN({symbol:e,isOpen:t,onClick:n}){return r.jsx("button",{type:"button",className:"pair-name",onClick:n,"aria-expanded":t,title:"?? TradingView K??",style:{background:"none",border:"none",padding:0,margin:0,color:"inherit",cursor:"pointer",textAlign:"left",fontFamily:"inherit"},children:e})}function Eu({wlItem:e,scanItem:t,removeBtn:n,showExLink:o=!0,chartOpen:u=!1,onToggleChart:m}){const i=e.symbol,a=e.exchangeId,s=e.exchangeLabel||a.toUpperCase(),l={binance:`https://www.binance.com/zh-CN/futures/${i.replace("/","")}`,okx:`https://www.okx.com/trade-swap/${i.replace("/","-").toLowerCase()}-swap`,bybit:`https://www.bybit.com/trade/usdt/${i.replace("/USDT","")}`,bitget:`https://www.bitget.com/futures/usdt/${i.replace("/","")}`,gate:`https://www.gate.io/futures_trade/USDT/${i.replace("/USDT","_USDT")}`,kucoin:`https://www.kucoin.com/futures/trade/${i.replace("/USDT","USDTM")}`}[a];if(!t)return r.jsxs("tr",{children:[r.jsx("td",{children:n}),r.jsxs("td",{children:[r.jsx(IKTVN,{symbol:i,isOpen:u,onClick:m}),r.jsx("span",{style:{fontSize:12,color:"#888"},children:s})]}),r.jsx("td",{style:{color:"var(--text-secondary)"},children:"加载中"}),r.jsx("td",{children:"-"}),r.jsx("td",{children:"-"}),r.jsx("td",{style:{textAlign:"right"},children:r.jsx("span",{style:{color:"#555",fontSize:12},children:"等待数据"})})]});let c=r.jsx("span",{style:{color:"#555",fontSize:12},children:"观望 Wait"});return t.signal!=="-"&&(t.action==="LONG"?c=o&&l?r.jsx("a",{href:l,target:"_blank",rel:"noreferrer",className:"btn-action btn-long",style:{textDecoration:"none",display:"inline-block",color:"#fff"},children:"开多 LONG"}):r.jsx("span",{className:"btn-action btn-long",style:{display:"inline-block",cursor:"default"},children:"开多 LONG"}):c=o&&l?r.jsx("a",{href:l,target:"_blank",rel:"noreferrer",className:"btn-action btn-short",style:{textDecoration:"none",display:"inline-block",color:"#fff"},children:"开空 SHORT"}):r.jsx("span",{className:"btn-action btn-short",style:{display:"inline-block",cursor:"default"},children:"开空 SHORT"})),r.jsxs("tr",{children:[r.jsx("td",{children:n}),r.jsxs("td",{children:[r.jsx(IKTVN,{symbol:i,isOpen:u,onClick:m}),r.jsx("span",{style:{fontSize:12,color:"#888"},children:s})]}),r.jsx("td",{children:r.jsx(Wm,{trend:t.trend})}),r.jsx("td",{children:r.jsx($m,{signal:t.signal,action:t.action})}),r.jsx("td",{style:{color:"#888",fontSize:13},children:t.detail}),r.jsx("td",{style:{textAlign:"right"},children:c})]})}const Hm=({onClick:e})=>r.jsx("button",{style:{background:"none",border:"none",cursor:"pointer",fontSize:16,color:"var(--accent)",padding:"4px 8px"},onClick:e,children:r.jsx("svg",{width:"18",height:"18",viewBox:"0 0 24 24",fill:"currentColor",stroke:"none",children:r.jsx("polygon",{points:"12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"})})}),zu=({onClick:e})=>r.jsx("button",{style:{background:"none",border:"none",cursor:"pointer",fontSize:16,color:"var(--binance-red)",padding:"4px 8px"},onClick:e,children:"×"}),_u=()=>r.jsx("thead",{children:r.jsxs("tr",{children:[r.jsx("th",{style:{width:40}}),r.jsx("th",{children:"交易标的 / 平台"}),r.jsx("th",{children:"趋势概览 (EMA200)"}),r.jsx("th",{children:"策略信号"}),r.jsx("th",{children:"信号详情"}),r.jsx("th",{style:{textAlign:"right"},children:"操作建议"})]})});function rc({watchlist:e,scanData:t,exchangeLabel:n,watchlistMode:o,searchQ:i,setSearchQ:a,showDropdown:s,setShowDropdown:l,searchWrapRef:c,filteredSymbols:d,addSymbol:h,removeSymbol:f}){const g={};t.forEach(v=>{g[v.symbol]=v});const y=o==="movers",b=x.useState(""),z=b[0],p=b[1];return r.jsxs("div",{className:"list-container",children:[y?r.jsxs("div",{style:{padding:"12px 20px",background:"rgba(77, 184, 255, 0.1)",border:"1px solid rgba(77, 184, 255, 0.2)",borderRadius:8,marginBottom:16,display:"flex",alignItems:"center",gap:12},children:[r.jsx("div",{style:{width:8,height:8,borderRadius:"50%",background:"var(--binance-green)",boxShadow:"0 0 10px var(--binance-green)"}}),r.jsx("span",{style:{color:"var(--accent)",fontWeight:600,fontSize:14},children:"今日市场异动 (Binance Top 5 Gainers & Losers)"}),r.jsx("span",{style:{color:"var(--text-secondary)",fontSize:12},children:"每 5 分钟自动更新"})]}):r.jsx(Tu,{wrapRef:c,value:i,onChange:v=>{a(v.target.value),l(!0)},onFocus:()=>l(!0),onKeyDown:v=>{v.key==="Enter"&&d.length>0&&h(d[0])},showDropdown:s,filteredSymbols:d,inListFn:v=>e.some(b=>b.key===v),onAdd:h}),r.jsxs("table",{className:"bn-table",children:[r.jsx(_u,{}),r.jsx("tbody",{children:e.length===0?r.jsx("tr",{children:r.jsxs("td",{colSpan:6,style:{textAlign:"center",padding:40},children:[r.jsxs("div",{style:{color:"var(--binance-green)",fontSize:16,fontWeight:600,marginBottom:10},children:[n," 数据已加载完毕"]}),r.jsx("div",{style:{color:"var(--text-secondary)",fontSize:14},children:y?"正在加载异动标的...":"请通过上方搜索框添加标的开始监控 (最多20个)"})]})}):e.map(v=>r.jsxs(r.Fragment,{children:[r.jsx(Eu,{wlItem:v,scanItem:g[v.symbol],showExLink:!0,chartOpen:z===v.key,onToggleChart:()=>p(z===v.key?"":v.key),removeBtn:y?r.jsx(Hm,{onClick:()=>h(v)}):r.jsx(zu,{onClick:()=>f(v.key)})}),z===v.key&&r.jsxs("tr",{children:[r.jsx("td",{}),r.jsx("td",{colSpan:5,style:{paddingTop:0},children:r.jsx(IKTVB,{wlItem:v})})]})]},v.key))})]})]})}function Km({group:e,scanData:t,exchangeLabel:n,groupSearchQ:o,setGroupSearchQ:i,groupShowDropdown:a,setGroupShowDropdown:s,groupSearchWrapRef:l,filteredGroupSymbols:c,addSymbolToGroup:d,removeSymbolFromGroup:h}){const f={};t.forEach(y=>{f[y.symbol]=y});const g=e.symbols||[],m=x.useState(""),j=m[0],T=m[1];return r.jsxs("div",{className:"list-container",children:[r.jsx(Tu,{wrapRef:l,value:o,onChange:y=>{i(y.target.value),s(!0)},onFocus:()=>s(!0),onKeyDown:y=>{y.key==="Enter"&&c.length>0&&d(e.id,c[0])},showDropdown:a,filteredSymbols:c,inListFn:y=>{const v=y.includes("@")?y.split("@")[0]:y;return g.includes(v)},onAdd:y=>d(e.id,y)}),r.jsxs("table",{className:"bn-table",children:[r.jsx(_u,{}),r.jsx("tbody",{children:g.length===0?r.jsx("tr",{children:r.jsxs("td",{colSpan:6,style:{textAlign:"center",padding:40},children:[r.jsx("div",{style:{color:"var(--binance-green)",fontSize:16,fontWeight:600,marginBottom:10},children:"数据已加载完毕"}),r.jsxs("div",{style:{color:"var(--text-secondary)",fontSize:14},children:["通过上方搜索框向「",e.name,"」添加标的"]})]})}):g.map(y=>{const v={key:y,symbol:y,exchangeId:"binance",display:y,exchangeLabel:n};return r.jsxs(r.Fragment,{children:[r.jsx(Eu,{wlItem:v,scanItem:f[y],showExLink:!1,chartOpen:j===y,onToggleChart:()=>T(j===y?"":y),removeBtn:r.jsx(zu,{onClick:()=>h(e.id,y)})}),j===y&&r.jsxs("tr",{children:[r.jsx("td",{}),r.jsx("td",{colSpan:5,style:{paddingTop:0},children:r.jsx(IKTVB,{wlItem:v})})]})]},y)})})]})]})}function Vm({alertLog:e,setAlertLog:t,unreadCount:n,setUnreadCount:o,markAllRead:i}){return r.jsxs("div",{className:"list-container",children:[r.jsxs("div",{style:{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"8px 12px",marginBottom:10,borderBottom:"1px solid var(--border-color)"},children:[r.jsx("span",{style:{fontSize:14,color:"var(--text-secondary)"},children:"信号快讯记录"}),r.jsxs("div",{style:{display:"flex",gap:8},children:[r.jsx("button",{className:"btn-action",style:{width:"auto",padding:"5px 12px",fontSize:12,background:"var(--input-bg)",color:"var(--text-primary)"},onClick:i,children:"全部已读"}),r.jsx("button",{className:"btn-action",style:{width:"auto",padding:"5px 12px",fontSize:12,background:"var(--input-bg)",color:"var(--text-primary)"},onClick:()=>{t([]),o(0)},children:"清除日志"})]})]}),r.jsx("div",{style:{maxHeight:600,overflowY:"auto"},children:e.length===0?r.jsx("div",{style:{textAlign:"center",padding:40,color:"var(--text-secondary)"},children:"暂无快讯记录"}):(()=>{let a="";return e.map((s,l)=>{const c=s.date!==a;c&&(a=s.date);const d=s.action==="LONG",h=s.action==="SHORT",f=d?"#0ECB81":h?"#F6465D":"var(--text-secondary)",g=d?"LONG":h?"SHORT":s.action;return r.jsxs(lr.Fragment,{children:[c&&r.jsx("div",{style:{padding:"8px 14px",fontSize:12,color:"var(--text-secondary)",fontWeight:600},children:s.date}),r.jsxs("div",{className:`alert-log-item ${s.read?"":"unread"}`,onClick:()=>{s.read||(t(y=>y.map((v,b)=>b===l?{...v,read:!0}:v)),o(y=>Math.max(0,y-1)))},children:[r.jsxs("div",{className:"al-title",children:[s.signal," ",s.symbol]}),r.jsxs("div",{className:"al-sub",children:[r.jsxs("svg",{width:"14",height:"14",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{marginRight:4,verticalAlign:-2},children:[r.jsx("line",{x1:"18",y1:"20",x2:"18",y2:"10"}),r.jsx("line",{x1:"12",y1:"20",x2:"12",y2:"4"}),r.jsx("line",{x1:"6",y1:"20",x2:"6",y2:"14"})]}),s.timeframe," · ",s.detail," · ",r.jsx("span",{style:{color:f,fontWeight:"bold"},children:g})," · ",s.time]})]})]},s.id||l)})})()})]})}const Gm=[["api","API 连接"],["email","邮箱配置"],["etpl","邮件模板"],["sound","自定义音效"]];function Xm({modalTab:e,setModalTab:t,onClose:n,onSave:o,cfg:i,setCfg:a,emailTemplate:s,setEmailTemplate:l,customSounds:c,soundName:d,setSoundName:h,soundFileRef:f,uploadSound:g,deleteSound:y,sendTestEmail:v,testEmailLoading:b,testEmailResult:z}){return r.jsx("div",{className:"modal",onClick:p=>p.target===p.currentTarget&&n(),children:r.jsxs("div",{className:"modal-container",children:[r.jsx("div",{className:"modal-sidebar",children:Gm.map(([p,u])=>r.jsx("div",{className:`modal-tab ${e===p?"active":""}`,onClick:()=>t(p),children:u},p))}),r.jsxs("div",{className:"modal-content",children:[e==="api"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"modal-title",children:"API & 网络连接"}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"代理地址 Proxy"}),r.jsx("input",{className:"bn-input",value:i.proxy,onChange:p=>a(u=>({...u,proxy:p.target.value})),placeholder:"例: 127.0.0.1:7890"}),r.jsx("div",{style:{fontSize:12,color:"var(--accent,#4db8ff)",marginTop:5},children:"本地运行必填，云服务器不填。"})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"Binance API Key"}),r.jsx("input",{type:"password",className:"bn-input",value:i.apiKey,onChange:p=>a(u=>({...u,apiKey:p.target.value})),placeholder:"输入 API Key"})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"Secret Key"}),r.jsx("input",{type:"password",className:"bn-input",value:i.secretKey,onChange:p=>a(u=>({...u,secretKey:p.target.value})),placeholder:"输入 Secret Key"})]}),r.jsxs("div",{className:"control-group",style:{marginTop:16},children:[r.jsxs("span",{className:"label",style:{display:"flex",alignItems:"center",gap:6},children:["豆包 AI Key",r.jsx("a",{href:"https://console.volcengine.com/ark/region:ark+cn-beijing/apiKey",target:"_blank",rel:"noreferrer",style:{fontSize:11,color:"var(--accent,#4db8ff)",textDecoration:"none",opacity:.8},children:"申请免费额度 →"})]}),r.jsx("input",{type:"password",className:"bn-input",value:i.doubaoApiKey||"",onChange:p=>a(u=>({...u,doubaoApiKey:p.target.value})),placeholder:"填入火山方舟 API Key"}),r.jsx("div",{style:{fontSize:11,color:"var(--text-secondary,#9ca3af)",marginTop:4},children:"新用户赠送 1000万 Tokens，指标页 AI 对话使用此 Key"})]})]}),e==="email"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"modal-title",children:"邮箱发送者配置 (SMTP)"}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"邮箱 (Email)"}),r.jsx("input",{type:"email",className:"bn-input",value:i.email,onChange:p=>a(u=>({...u,email:p.target.value})),placeholder:"example@gmail.com"})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"应用专用密码 (App Password)"}),r.jsx("input",{type:"password",className:"bn-input",value:i.emailPass,onChange:p=>a(u=>({...u,emailPass:p.target.value})),placeholder:"16位应用密码"})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("button",{className:"btn-action",style:{background:"#0ecb81",color:"#001a0e",width:"100%",fontSize:13,fontWeight:700},onClick:v,disabled:b,children:b?"发送中...":"发送测试邮件"}),z&&r.jsx("div",{style:{fontSize:12,marginTop:6,color:z.startsWith("发送成功")?"#0ECB81":"#F6465D"},children:z})]})]}),e==="etpl"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"modal-title",children:"邮件推送内容模板"}),[["include_price","交易标的 / 价格","包含币种名称和当前价格"],["include_trend","趋势概览 (EMA200)","多头/空头方向判断"],["include_signal","策略信号","当前指标的策略信号"],["include_detail","信号详情","红柱缩短/绿柱增强等"],["include_action","操作建议","做多/做空建议"]].map(([p,u,m])=>r.jsx("div",{className:"tv-check-row",children:r.jsxs("div",{className:"tv-check-left",children:[r.jsx("input",{type:"checkbox",className:"tv-check-box",checked:!!s[p],onChange:j=>l(T=>({...T,[p]:j.target.checked}))}),r.jsxs("div",{className:"tv-check-info",children:[r.jsx("h4",{children:u}),r.jsx("p",{children:m})]})]})},p))]}),e==="sound"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"modal-title",children:"自定义提示音效"}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"音效名称"}),r.jsx("input",{className:"bn-input",value:d,onChange:p=>h(p.target.value),placeholder:"例: 我的铃声"})]}),r.jsxs("div",{className:"control-group",children:[r.jsx("span",{className:"label",children:"上传 MP3 文件"}),r.jsx("input",{ref:f,type:"file",accept:".mp3",style:{color:"var(--text-primary)",fontSize:13}})]}),r.jsx("button",{className:"btn-action",style:{background:"var(--binance-green)",width:"100%",marginBottom:20},onClick:g,children:"⬆️ 上传音效"}),r.jsx("div",{className:"label",children:"已上传的音效"}),r.jsx("div",{style:{maxHeight:220,overflowY:"auto"},children:c.length===0?r.jsx("div",{style:{color:"var(--text-secondary)",fontSize:13,padding:10},children:"暂无自定义音效"}):c.map(p=>r.jsxs("div",{style:{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"8px 0",borderBottom:"1px solid var(--border-color)"},children:[r.jsx("span",{style:{fontSize:14},children:p.name}),r.jsxs("div",{style:{display:"flex",gap:8},children:[r.jsxs("button",{className:"btn-action",style:{width:"auto",padding:"4px 10px",fontSize:12,background:"var(--input-bg)",color:"var(--text-primary)"},onClick:()=>{const u=new Audio("/api/sound/"+p.file);u.volume=.5,u.play()},children:[r.jsx("svg",{width:"12",height:"12",viewBox:"0 0 24 24",fill:"currentColor",style:{marginRight:4},children:r.jsx("polygon",{points:"5 3 19 12 5 21 5 3"})}),"试听"]}),r.jsx("button",{className:"btn-action",style:{width:"auto",padding:"4px 10px",fontSize:12,background:"var(--binance-red)"},onClick:()=>y(p.file),children:"删除"})]})]},p.file))})]}),r.jsx("div",{style:{marginTop:30},children:r.jsx("button",{className:"btn-action",style:{background:"var(--accent)",color:"#000",width:"100%",height:40,fontSize:15},onClick:o,children:"保存并生效"})})]})]})})}const Ia={binance:"Binance",okx:"OKX",bybit:"Bybit",bitget:"Bitget",gate:"Gate.io",kucoin:"KuCoin"},Qm=["BTC","ETH","BNB","SOL","XRP","DOGE","ADA","AVAX","LINK","DOT","MATIC","UNI","LTC","BCH","ATOM","ETC","XLM","ALGO","ICP","FIL","APT","ARB","OP","SUI","SEI","TIA","INJ","WLD","BLUR","PEPE","FLOKI","SHIB","BONE","LDO","AAVE","CRV","MKR","SNX","COMP","YFI","RUNE","NEAR","FTM","ONE","VET","THETA","EOS","XTZ","ZEC","DASH","SAND","MANA","AXS","ENJ","CHZ","GALA","IMX","RNDR","GRT","1INCH","DYDX","GMX","PENDLE","PYTH","JTO","MEME","BONK","WIF","BOME","MYRO","ORDI","SATS","RATS","LUNC","USTC","CFX","STX","AGIX","FET","OCEAN","ONDO","JUP","STRK","ALT","PIXEL","PORTAL","MANTA","ZK","EIGEN","LISTA","TON","NOT","DOGS","HMSTR","CATI","MAJOR","WEN","POPCAT","TURBO","NEIRO"],__ikunScope=()=>{const e=(localStorage.getItem("ikun_token")||"anon").replace(/[^a-zA-Z0-9_.-]/g,"_").slice(0,80)||"anon";return e},Ru=e=>`ikun_wl_${__ikunScope()}_${e||"binance"}`;function nr(e){try{const t=localStorage.getItem(Ru(e));return JSON.parse(t!==null?t:__ikunScope()==="anon"?localStorage.getItem(`ikun_wl_${e||"binance"}`)||"[]":"[]")}catch{return[]}}function oc(e,t){localStorage.setItem(Ru(e),JSON.stringify(t))}function Fi(e,t){const n=Ia[t]||t.toUpperCase();return{key:`${e}/USDT@${t}`,symbol:`${e}/USDT`,exchangeId:t,display:`${e}USDT.P`,exchangeLabel:n}}function Ym(e,t,n,o="binance"){const[i,a]=x.useState("favorites"),[s,l]=x.useState([]),[c,d]=x.useState([]),[h,f]=x.useState(()=>nr(o)),[g,y]=x.useState([]),[v,b]=x.useState("就绪"),[z,p]=x.useState([]),u=x.useRef(o);x.useEffect(()=>{fetch("/api/all_symbols").then(D=>D.json()).then(D=>{Array.isArray(D)&&p(D)}).catch(()=>{})},[o]);const m=x.useCallback(()=>{fetch("/api/market_movers").then(D=>D.json()).then(D=>{if(D.status==="success"){const $=D.data.map(F=>Fi(F.symbol.replace("/USDT",""),"binance"));l($)}}).catch(()=>{})},[]);x.useEffect(()=>{m();const D=setInterval(m,3e5);return()=>clearInterval(D)},[m]),x.useEffect(()=>{u.current!==o&&(u.current=o,f(nr(o)),y([]),b("就绪"),T(""),C(!1))},[o]),x.useEffect(()=>{const D=()=>{f(nr(o)),y([])};return window.addEventListener("ikun_wl_sync",D),()=>window.removeEventListener("ikun_wl_sync",D)},[o]);const[j,T]=x.useState(""),[w,C]=x.useState(!1),_=x.useRef(null),[P,N]=x.useState(""),[B,K]=x.useState(!1),le=x.useRef(null),Q=x.useRef(""),ye=Ia[o]||o.toUpperCase();x.useEffect(()=>{const D=$=>{_.current&&!_.current.contains($.target)&&C(!1),le.current&&!le.current.contains($.target)&&K(!1)};return document.addEventListener("mousedown",D),()=>document.removeEventListener("mousedown",D)},[]),x.useEffect(()=>{const D=setInterval(()=>{if((i==="favorites"?h:s).length===0)return;const F=new Date,W=F.getMinutes(),Y=F.getSeconds(),te=F.getHours(),Ee={"15m":900,"1h":3600,"4h":14400,"1d":86400}[e]||3600;let Pe=0;e==="15m"?Pe=W%15*60+Y:e==="1h"?Pe=W*60+Y:e==="4h"?Pe=te%4*3600+W*60+Y:Pe=te*3600+W*60+Y;const fe=Ee-Pe,Ge=`${e}:${Math.floor(Date.now()/(Ee*1e3))}`;if(Pe<=10&&Q.current!==Ge)Q.current=Ge,pe();else if(Pe>10){const we=fe>60?`${Math.ceil(fe/60)}分钟`:`${fe}秒`;b(`下次扫描: ${we}后`)}},5e3);return()=>clearInterval(D)},[e,t,h,s,i]);const pe=x.useCallback(()=>{const D=i==="favorites"?h:s,$=i==="favorites"?o:"binance";if(D.length===0){i==="favorites"?y([]):d([]),b("就绪");return}b("扫描中...");const F=D.map(W=>W.symbol).join(",");fetch(`/api/scan?timeframe=${e}&trigger=${t}&exchange=${$}&symbols=${encodeURIComponent(F)}`).then(W=>W.json()).then(W=>{const Y=(W.data||[]).sort((Ee,Pe)=>{const fe=D.findIndex(we=>we.symbol===Ee.symbol),Ge=D.findIndex(we=>we.symbol===Pe.symbol);return(fe===-1?9999:fe)-(Ge===-1?9999:Ge)});i==="favorites"?y(Y):d(Y),b(`已连接 ${i==="favorites"?ye:"Binance"}`)}).catch(()=>b("连接失败"))},[h,s,i,e,t,o,ye]);x.useEffect(()=>{pe()},[pe]);function I(D){const $=D.exchangeId||o;let F;if(typeof D=="string"?F=D.toUpperCase().replace(/\/USDT.*/,"").replace("USDT","").replace(".P","").trim():F=(D.base||D.symbol||"").toUpperCase().replace(/\/USDT.*/,"").replace("USDT","").replace(".P","").trim(),!F)return;const W=Fi(F,$),Y=nr($);if(Y.find(Ee=>Ee.key===W.key)){n(`${W.display} 已在 ${W.exchangeLabel} 自选中`);return}if(Y.length>=20){n(`${W.exchangeLabel} 自选上限20个，请先移除`);return}const te=[...Y,W];oc($,te),$===o&&(f(te),T(""),C(!1)),n(`已添加 ${W.display} · ${W.exchangeLabel}`),fetch("/api/add_symbol",{method:"POST",headers:{"Content-Type":"application/json",...((localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{})},body:JSON.stringify({symbol:W.symbol,exchange:$,timeframe:e})}).catch(()=>{})}function S(D){const[$,F]=D.includes("@")?D.split("@"):[D,o],Y=nr(F).filter(te=>te.key!==D);oc(F,Y),F===o&&(f(Y),y(te=>te.filter(Ee=>Ee.symbol!==$))),n(`已移除 ${$} · ${Ia[F]||F}`),fetch("/api/remove_symbol",{method:"POST",headers:{"Content-Type":"application/json",...((localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{})},body:JSON.stringify({symbol:$,exchange:F,timeframe:e})}).catch(()=>{})}function R(D){if(!D)return[];const $=D.toUpperCase().replace(/\/USDT.*/,"").replace("USDT","").replace(".P","").trim();return $?(z.length>0?z:Qm).filter(W=>W.toUpperCase().startsWith($)||W.toUpperCase().includes($)).slice(0,20).map(W=>Fi(W,o)):[]}const M=R(j),U=R(P);return{watchlist:h,setWatchlist:f,marketMovers:s,moversScanData:c,watchlistMode:i,setWatchlistMode:a,scanData:g,setScanData:y,connStatus:v,setConnStatus:b,searchQ:j,setSearchQ:T,showDropdown:w,setShowDropdown:C,searchWrapRef:_,groupSearchQ:P,setGroupSearchQ:N,groupShowDropdown:B,setGroupShowDropdown:K,groupSearchWrapRef:le,filteredSymbols:M,filteredGroupSymbols:U,fetchData:pe,addSymbol:I,removeSymbol:S,symbolsLoaded:!0,allSymbols:z}}function Jm(e,t,n){const _k=e=>{const t=(localStorage.getItem("ikun_token")||"anon").replace(/[^a-zA-Z0-9_.-]/g,"_").slice(0,80)||"anon";return e+"_"+t},[o,i]=x.useState(()=>{try{return JSON.parse(localStorage.getItem(_k("ikun_alertlog"))||"[]")}catch{return[]}}),[a,s]=x.useState(()=>{try{const z=parseInt(localStorage.getItem(_k("ikun_unread"))||"0",10),p=JSON.parse(localStorage.getItem(_k("ikun_alertlog"))||"[]");return Math.min(z,p.length)}catch{return 0}}),[l,c]=x.useState(0),[d,h]=x.useState([]),f=x.useRef(t);x.useEffect(()=>{f.current=t},[t]),x.useEffect(()=>{localStorage.setItem(_k("ikun_alertlog"),JSON.stringify(o)),localStorage.setItem(_k("ikun_unread"),String(a))},[o,a]),x.useEffect(()=>{const z=setInterval(g,3e3);return()=>clearInterval(z)},[l]),x.useEffect(()=>{if(d.length===0)return;const z=setTimeout(()=>h([]),15e3);return()=>clearTimeout(z)},[d]),x.useEffect(()=>{v()},[]);function g(){fetch("/api/web_signals?since="+l,{headers:(localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{}}).then(z=>z.json()).then(z=>{const p=z.signals||[];if(p.length===0)return;let u=l;p.forEach(m=>{m._ts>u&&(u=m._ts)}),c(u),i(m=>{const j=[];if(p.forEach(w=>{const C=w.trigger_time_full||"";if(!m.find(P=>P.symbol===w.symbol&&P.action===w.action&&P.trigger_time_full===C)){const P=new Date;j.push({symbol:w.symbol,signal:w.type||"-",detail:w.detail||"-",action:w.action,timeframe:w.timeframe||"",trigger_time:w.trigger_time||"",trigger_time_full:C,time:w.trigger_time||P.toLocaleTimeString("zh-CN",{hour:"2-digit",minute:"2-digit"}),date:P.getMonth()+1+"月"+P.getDate()+"日",read:!1,id:Date.now()+Math.random()})}}),j.length===0)return m;const T=f.current;return T.sound&&Cu(T.sound_type),T.toast&&h(j),s(w=>w+j.length),[...j,...m].slice(0,300)})}).catch(()=>{})}function y(z){i(p=>{const u=new Date,m=u.getMonth()+1+"月"+u.getDate()+"日",j=[];return z.forEach(T=>{const w=T.trigger_time||u.toLocaleTimeString("zh-CN",{hour:"2-digit",minute:"2-digit"});p.find(_=>_.symbol===T.symbol&&_.action===T.action&&_.time===w)||j.push({symbol:T.symbol,signal:T.signal,detail:T.detail,action:T.action,timeframe:T.timeframe||e,time:w,trigger_time_full:T.trigger_time_full||"",date:m,read:!1,id:Date.now()+Math.random()})}),j.length===0?p:(s(T=>T+j.length),[...j,...p].slice(0,300))})}function v(){const z=localStorage.getItem(_k("ikun_last_visit"))||"";localStorage.setItem(_k("ikun_last_visit"),new Date().toISOString().replace("T"," ").substring(0,19)),z&&fetch("/api/signal_history?since="+encodeURIComponent(z)).then(p=>p.json()).then(p=>{const u=p.signals||[];if(u.length===0)return;const m=u.map(j=>({symbol:j.symbol,signal:j.type,detail:j.detail,action:j.action,timeframe:j.timeframe||"",trigger_time:j.trigger_time||"",trigger_time_full:j.trigger_time_full||""}));y(m),n("离线期间收到 "+u.length+" 条信号")}).catch(()=>{})}function b(){i(z=>z.map(p=>({...p,read:!0}))),s(0)}return{alertLog:o,setAlertLog:i,unreadCount:a,setUnreadCount:s,notifyItems:d,setNotifyItems:h,addToAlertLogItems:y,markAllRead:b}}function Zm({exchange:e,symbols:t,timeframe:n,onSignal:o,enabled:i=!0}){const a=x.useRef(null),s=x.useRef(null),l=x.useRef(2e3),c=x.useRef(o),d=x.useRef(t),[h,f]=x.useState("disconnected");x.useEffect(()=>{c.current=o},[o]),x.useEffect(()=>{d.current=t,!(!i||!t||t.length===0)&&t.forEach(y=>{fetch("/api/ws/subscribe",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({exchange:e,symbol:y,timeframe:n})}).catch(()=>{})})},[t,e,n,i]);const g=x.useCallback(()=>{if(!i)return;a.current&&(a.current.close(),a.current=null),clearTimeout(s.current);const y=d.current;if(!y||y.length===0){f("disconnected");return}const v=encodeURIComponent(y.join(",")),m=localStorage.getItem("ikun_token")||"",b="/api/stream?exchange="+e+"&symbols="+v+"&timeframe="+n+(m?"&token="+encodeURIComponent(m):"");f("connecting");const z=new EventSource(b);a.current=z,z.onopen=()=>{f("connected"),l.current=2e3},z.onmessage=p=>{var u;try{const m=JSON.parse(p.data);(u=c.current)==null||u.call(c,m)}catch{}},z.onerror=()=>{f("reconnecting"),z.close(),a.current=null,s.current=setTimeout(()=>{l.current=Math.min(l.current*1.5,3e4),g()},l.current)}},[e,n,i]);return x.useEffect(()=>(clearTimeout(s.current),g(),()=>{var y;clearTimeout(s.current),(y=a.current)==null||y.close(),a.current=null}),[g]),x.useEffect(()=>{i&&t&&t.length>0&&!a.current&&g()},[t,i,g]),{streamStatus:h}}const qm={binance:"Binance",okx:"OKX",bybit:"Bybit",bitget:"Bitget",gate:"Gate.io",kucoin:"KuCoin"};function eg(e){try{const t=new(window.AudioContext||window.webkitAudioContext);(e==="LONG"?[440,554,659]:[659,554,440]).forEach((o,i)=>{const a=t.createOscillator(),s=t.createGain();a.connect(s),s.connect(t.destination),a.type="sine",a.frequency.value=o;const l=t.currentTime+i*.12;s.gain.setValueAtTime(0,l),s.gain.linearRampToValueAtTime(.25,l+.02),s.gain.exponentialRampToValueAtTime(.001,l+.25),a.start(l),a.stop(l+.28)})}catch{}}let Ui=null;function tg(e,t){const n=document.title,o=e==="LONG"?`▲ LONG ${t}`:`▼ SHORT ${t}`;let i=!0,a=0;clearInterval(Ui),Ui=setInterval(()=>{document.title=i?o:n,i=!i,++a>=12&&(clearInterval(Ui),document.title=n)},500)}function ng({signal:e,onClose:t}){var s,l;const n=e.action==="LONG",o=qm[e.exchange]||((s=e.exchange)==null?void 0:s.toUpperCase())||"",i=n?"#00e5a0":"#ff4d6d",a=n?"linear-gradient(135deg, rgba(0,229,160,0.12) 0%, rgba(0,40,30,0.95) 100%)":"linear-gradient(135deg, rgba(255,77,109,0.12) 0%, rgba(40,0,15,0.95) 100%)";return r.jsxs("div",{style:{position:"relative",background:a,border:`1px solid ${i}44`,borderLeft:`3px solid ${i}`,borderRadius:10,padding:"14px 16px",minWidth:300,maxWidth:380,backdropFilter:"blur(16px)",boxShadow:`0 4px 32px ${i}22, 0 2px 8px rgba(0,0,0,0.6)`,animation:"ikun-toast-in 0.35s cubic-bezier(.16,1,.3,1) forwards",cursor:"default"},children:[r.jsx("button",{onClick:t,style:{position:"absolute",top:8,right:10,background:"none",border:"none",cursor:"pointer",color:"#666",fontSize:16,lineHeight:1,padding:"2px 4px"},children:"×"}),r.jsxs("div",{style:{display:"flex",alignItems:"center",gap:10,marginBottom:8},children:[r.jsx("span",{style:{background:i,color:"#000",fontWeight:800,fontSize:11,padding:"3px 8px",borderRadius:4,letterSpacing:1},children:n?"▲ LONG":"▼ SHORT"}),r.jsxs("span",{style:{fontWeight:700,fontSize:16,color:"#fff",letterSpacing:.5},children:[(l=e.symbol)==null?void 0:l.replace("/USDT",""),r.jsx("span",{style:{color:"#666",fontWeight:400,fontSize:13},children:"/USDT"})]}),r.jsx("span",{style:{marginLeft:"auto",fontSize:11,color:"#555",fontFamily:"monospace"},children:e.timeframe})]}),r.jsxs("div",{style:{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6},children:[r.jsx("span",{style:{fontSize:13,color:i,fontWeight:600},children:e.type}),r.jsx("span",{style:{fontSize:12,color:"#888"},children:e.detail})]}),r.jsxs("div",{style:{display:"flex",justifyContent:"space-between",alignItems:"center"},children:[r.jsxs("span",{style:{fontSize:15,fontWeight:700,color:"#fff",fontFamily:"monospace"},children:["$",typeof e.price=="number"?e.price.toLocaleString("en",{maximumFractionDigits:4}):e.price]}),r.jsxs("div",{style:{display:"flex",gap:6,alignItems:"center"},children:[r.jsx("span",{style:{fontSize:11,color:"#555",background:"#1a1a1a",padding:"2px 6px",borderRadius:4},children:e.trend}),r.jsx("span",{style:{fontSize:11,color:"#666"},children:o})]})]}),r.jsx("div",{style:{position:"absolute",bottom:0,left:0,right:0,height:2,background:`${i}33`,borderRadius:"0 0 10px 10px",overflow:"hidden"},children:r.jsx("div",{style:{height:"100%",background:i,animation:"ikun-progress 8s linear forwards",transformOrigin:"left"}})})]})}const Lu=5;function rg({signals:e,onDismiss:t}){return!e||e.length===0?null:r.jsx("div",{style:{position:"fixed",top:70,right:20,zIndex:9999,display:"flex",flexDirection:"column",gap:10,pointerEvents:"none"},children:e.slice(0,Lu).map(n=>r.jsx("div",{style:{pointerEvents:"auto"},children:r.jsx(ng,{signal:n,onClose:()=>t(n._uid)})},n._uid))})}let ic=!1;function og(){if(ic)return;ic=!0;const e=document.createElement("style");e.textContent=`
    @keyframes ikun-toast-in {
      from { opacity: 0; transform: translateX(60px) scale(0.92); }
      to   { opacity: 1; transform: translateX(0)   scale(1);    }
    }
    @keyframes ikun-progress {
      from { transform: scaleX(1); }
      to   { transform: scaleX(0); }
    }
  `,document.head.appendChild(e)}let ig=0;function ag({soundEnabled:e=!0}={}){const[t,n]=x.useState([]);og();const o=x.useCallback(a=>{var l;const s={...a,_uid:++ig};e&&eg(a.action),tg(a.action,(l=a.symbol)==null?void 0:l.replace("/USDT","")),n(c=>[s,...c].slice(0,Lu)),setTimeout(()=>{n(c=>c.filter(d=>d._uid!==s._uid))},8e3)},[e]),i=x.useCallback(a=>{n(s=>s.filter(l=>l._uid!==a))},[]);return{alerts:t,addAlert:o,dismiss:i}}const sg={binance:"Binance",okx:"OKX",bybit:"Bybit",bitget:"Bitget",gate:"Gate.io",huobi:"HTX",kucoin:"KuCoin"},lg=[{id:"orion",name:"Orion Terminal",tag:"OI",url:"https://screener.orionterminal.com/",color:"#4db8ff"}];function cg(){const[e,t]=lr.useState(()=>JSON.parse(localStorage.getItem("ik_oi_custom")||"[]")),[n,o]=lr.useState("orion"),[i,a]=lr.useState({}),s=[...lg,...e],l=s.find(f=>f.id===n)||s[0];function c(f){try{const g=f.target.contentDocument;if(!g||g.URL==="about:blank")return}catch{a(g=>({...g,[n]:!0}))}}function d(){const f=window.prompt("请输入工具名称 (例如：CoinGlass)");if(!f)return;const g=window.prompt("请输入网址 URL (例如：https://www.coinglass.com/)");if(!g)return;const y=g.startsWith("http")?g:"https://"+g,v={id:"custom_"+Date.now(),name:f,tag:"自定义",url:y,color:"#8b5cf6"},b=[...e,v];t(b),localStorage.setItem("ik_oi_custom",JSON.stringify(b)),o(v.id)}function h(f,g){if(f.stopPropagation(),!window.confirm("确定移除这个自定义工具吗？"))return;const y=e.filter(v=>v.id!==g);t(y),localStorage.setItem("ik_oi_custom",JSON.stringify(y)),n===g&&o("orion")}return r.jsxs("div",{style:{display:"flex",flexDirection:"column",height:"100%",overflow:"hidden"},children:[r.jsxs("div",{style:{display:"flex",gap:6,flexShrink:0,flexWrap:"wrap",padding:"10px 0 12px",borderBottom:"1px solid var(--border-color)"},children:[s.map(f=>r.jsxs("button",{onClick:()=>o(f.id),style:{display:"flex",alignItems:"center",gap:6,padding:"5px 14px",borderRadius:20,border:"none",cursor:"pointer",fontFamily:"'Rajdhani', sans-serif",fontWeight:700,fontSize:13,background:n===f.id?f.color:"var(--input-bg)",color:n===f.id?"#000":"var(--text-secondary)",transition:"all 0.15s",opacity:n===f.id?1:.7},children:[f.name,r.jsx("span",{style:{fontSize:10,fontWeight:700,letterSpacing:"0.04em",background:n===f.id?"rgba(0,0,0,0.18)":"var(--border-color)",color:n===f.id?"#000":"var(--text-secondary)",padding:"1px 6px",borderRadius:8},children:f.tag}),f.id.startsWith("custom_")&&r.jsx("div",{onClick:g=>h(g,f.id),style:{display:"flex",alignItems:"center",justifyContent:"center",width:14,height:14,borderRadius:"50%",background:"rgba(0,0,0,0.2)",color:"#fff",fontSize:10,marginLeft:2},children:"✕"})]},f.id)),r.jsx("button",{onClick:d,style:{display:"flex",alignItems:"center",gap:6,padding:"5px 14px",borderRadius:20,border:"1px dashed var(--border-color)",cursor:"pointer",fontFamily:"'Rajdhani', sans-serif",fontWeight:700,fontSize:13,background:"transparent",color:"var(--text-secondary)",transition:"all 0.15s"},onMouseEnter:f=>{f.currentTarget.style.color="var(--accent)",f.currentTarget.style.borderColor="var(--accent)"},onMouseLeave:f=>{f.currentTarget.style.color="var(--text-secondary)",f.currentTarget.style.borderColor="var(--border-color)"},children:"+ 添加工具"}),r.jsxs("a",{href:l.url,target:"_blank",rel:"noopener noreferrer",style:{marginLeft:"auto",display:"flex",alignItems:"center",gap:5,fontSize:12,color:"var(--text-secondary)",textDecoration:"none",padding:"5px 12px",borderRadius:20,border:"1px solid var(--border-color)",transition:"color 0.15s, border-color 0.15s"},onMouseEnter:f=>{f.currentTarget.style.color="var(--accent)",f.currentTarget.style.borderColor="var(--accent)"},onMouseLeave:f=>{f.currentTarget.style.color="var(--text-secondary)",f.currentTarget.style.borderColor="var(--border-color)"},children:[r.jsxs("svg",{width:"11",height:"11",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2.5",children:[r.jsx("path",{d:"M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"}),r.jsx("polyline",{points:"15 3 21 3 21 9"}),r.jsx("line",{x1:"10",y1:"14",x2:"21",y2:"3"})]}),"独立窗口"]})]}),r.jsx("div",{style:{flex:1,position:"relative",minHeight:0},children:i[n]?r.jsxs("div",{style:{height:"100%",display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",gap:16,color:"var(--text-secondary)"},children:[r.jsxs("svg",{width:"48",height:"48",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.5",opacity:"0.4",children:[r.jsx("circle",{cx:"12",cy:"12",r:"10"}),r.jsx("line",{x1:"4.93",y1:"4.93",x2:"19.07",y2:"19.07"})]}),r.jsxs("div",{style:{textAlign:"center"},children:[r.jsxs("div",{style:{fontSize:15,fontWeight:700,color:"var(--text-primary)",marginBottom:6},children:[l.name," 不允许内嵌"]}),r.jsx("div",{style:{fontSize:12,marginBottom:18},children:"该网站设置了安全策略，阻止了 iframe 加载"}),r.jsxs("a",{href:l.url,target:"_blank",rel:"noopener noreferrer",style:{display:"inline-flex",alignItems:"center",gap:6,background:l.color,color:"#000",padding:"9px 22px",borderRadius:20,fontFamily:"'Rajdhani', sans-serif",fontWeight:700,fontSize:14,textDecoration:"none"},children:["在新窗口打开 ",l.name," →"]})]})]}):r.jsx("iframe",{src:l.url,title:l.name,onLoad:c,style:{width:"100%",height:"100%",border:"none",borderRadius:8,background:"#000"},sandbox:"allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"},n)})]})}function dg({currentUser:e,onOpenLogin:t,doLogout:n,onNavigate:o,activePage:i,activeIndicator:a,onClearIndicator:s,externalExchange:l}){const[c,d]=x.useState("watchlist"),[h,f]=x.useState(!1),[g,y]=x.useState(!1),[v,b]=x.useState("api"),[z,p]=x.useState(""),[u,m]=x.useState(!!localStorage.getItem("ikun_cookie_ok")),[j,T]=x.useState(()=>{try{return JSON.parse(localStorage.getItem("ikun_listgroups")||"[]")}catch{return[]}}),[w,C]=x.useState(!1),[_,P]=x.useState(!1),[N,B]=x.useState("group"),[K,le]=x.useState(""),[Q,ye]=x.useState(""),[pe,I]=x.useState(""),[S,R]=x.useState(localStorage.getItem("ikun_token")||""),[M,U]=x.useState({apiKey:"",secretKey:"",email:"",emailPass:"",proxy:"",doubaoApiKey:""}),[D,$]=x.useState({app_push:!1,toast:!0,email:!1,sound:!1,sound_type:"beep"}),[F,W]=x.useState(()=>l||localStorage.getItem("ikun_exchange")||"binance");x.useEffect(()=>{l&&l!==F&&W(l)},[l]);const[Y,te]=x.useState(!1),[Ee,Pe]=x.useState({include_price:!0,include_trend:!0,include_signal:!0,include_detail:!0,include_action:!0}),[fe,Ge]=x.useState("15m"),[we,Wn]=x.useState("close"),[ft,St]=x.useState("favorites"),[De,Fe]=x.useState(""),[$n,Hn]=x.useState(!1),[Fr,Kn]=x.useState([]),[ln,E]=x.useState(""),V=lr.useRef(null),J=x.useCallback(k=>{p(k),setTimeout(()=>p(""),2500)},[]),cn=Ym(fe,we,J,F),{watchlist:dn,marketMovers:Vn,moversScanData:Kt,setWatchlistMode:_s,scanData:Rs,connStatus:Du,searchQ:Ls,setSearchQ:Ps,showDropdown:Ds,setShowDropdown:Is,searchWrapRef:Ms,groupSearchQ:Iu,setGroupSearchQ:As,groupShowDropdown:Mu,setGroupShowDropdown:Os,groupSearchWrapRef:Au,filteredSymbols:Bs,filteredGroupSymbols:Ou,fetchData:Fs,addSymbol:Us,removeSymbol:Ws}=cn;x.useEffect(()=>{_s(ft)},[ft,_s]);const{alertLog:Bu,setAlertLog:Fu,unreadCount:di,setUnreadCount:Uu,notifyItems:ui,setNotifyItems:$s,markAllRead:Wu}=Jm(fe,D,J),$u=sg[F]||F,{alerts:Hu,addAlert:Ku,dismiss:Vu}=ag({soundEnabled:D.sound!==!1}),Hs=dn.map(k=>typeof k=="string"?k:k.symbol);Zm({exchange:F,symbols:Hs,timeframe:fe,enabled:Hs.length>0,onSignal:Ku}),x.useEffect(()=>{e!==void 0&&(Gu(),Xu())},[e]);function Gu(){fetch("/api/get_settings",{headers:(localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{}}).then(k=>k.json()).then(k=>{if(U({apiKey:k.apiKey||"",secretKey:k.secretKey||"",email:k.email||"",emailPass:k.emailPass||"",proxy:k.proxy||"",doubaoApiKey:k.doubaoApiKey||""}),k.alertSettings&&$(O=>({...O,...k.alertSettings})),k.emailTemplate&&Pe(O=>({...O,...k.emailTemplate})),k.timeframe&&Ge(k.timeframe),k.triggerMode&&Wn(k.triggerMode),k.watchlistMode&&St(k.watchlistMode),k.exchangeId&&(W(k.exchangeId),localStorage.setItem("ikun_exchange",k.exchangeId)),k.watchlist&&Array.isArray(k.watchlist)&&k.watchlist.length>0){const O={binance:"Binance",okx:"OKX",bybit:"Bybit",bitget:"Bitget",gate:"Gate.io",kucoin:"KuCoin"},G={};k.watchlist.forEach(q=>{const he=typeof q=="string"?q:q.symbol,et=typeof q=="string"?k.exchangeId||"binance":q.exchange;G[et]||(G[et]=[]);const pi=he.replace("/USDT",""),Gn=O[et]||et.toUpperCase();G[et].push({key:he+"@"+et,symbol:he,exchangeId:et,display:pi+"USDT.P",exchangeLabel:Gn})}),Object.entries(G).forEach(([q,he])=>{localStorage.setItem(Ru(q),JSON.stringify(he))}),window.__ikun_wl_synced=!0,window.dispatchEvent(new Event("ikun_wl_sync"))}})}function Xu(){fetch("/api/list_sounds",{headers:(localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{}}).then(k=>k.json()).then(Kn)}function Qu(k,O,G,q){const he=O||fe,et=G||we,pi=q||ft;O&&O!==fe&&Ge(O),q&&q!==ft&&St(q);const Gn={...k};delete Gn._timeframe,$(Gn),fetch("/api/save_settings",{method:"POST",headers:{"Content-Type":"application/json",...((localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{})},body:JSON.stringify({alertSettings:Gn,timeframe:he,triggerMode:et,watchlistMode:pi})})}function Yu(){fetch("/api/save_settings",{method:"POST",headers:{"Content-Type":"application/json",...((localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{})},body:JSON.stringify({apiKey:M.apiKey,secretKey:M.secretKey,email:M.email,emailPass:M.emailPass,proxy:M.proxy,doubaoApiKey:M.doubaoApiKey,timeframe:fe,triggerMode:we,watchlistMode:ft,emailTemplate:Ee,alertSettings:D})}).then(k=>k.json()).then(()=>{J("配置已保存"),y(!1),Fs()})}function Ju(){Hn(!0),Fe(""),fetch("/api/save_settings",{method:"POST",headers:{"Content-Type":"application/json",...((localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{})},body:JSON.stringify({email:M.email,emailPass:M.emailPass})}).then(()=>fetch("/api/test_email",{method:"POST",headers:{"Content-Type":"application/json",...((localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{})},body:"{}"})).then(k=>k.json()).then(k=>{Fe((k.status==="success"?"发送成功: ":"发送失败: ")+k.msg),Hn(!1)}).catch(k=>{Fe("请求失败: "+((k==null?void 0:k.message)||"网络错误，请检查后端是否在线")),Hn(!1)})}function Zu(){var G;const k=(G=V.current)==null?void 0:G.files[0];if(!k){J("请选择MP3文件");return}const O=new FormData;O.append("file",k),O.append("name",ln||k.name.replace(".mp3","")),fetch("/api/upload_sound",{method:"POST",headers:(localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{},body:O}).then(q=>q.json()).then(q=>{if(q.status==="error"){J(q.msg);return}Kn(q.sounds||[]),E(""),V.current&&(V.current.value=""),J("音效已上传")})}function qu(k){fetch("/api/delete_sound",{method:"POST",headers:{"Content-Type":"application/json",...((localStorage.getItem("ikun_token")||"")?{"X-Token":localStorage.getItem("ikun_token")}:{})},body:JSON.stringify({file:k})}).then(O=>O.json()).then(O=>{Kn(O.sounds||[]),J("已删除")})}function ep(k,O){let G=O.toUpperCase().trim();G.includes(":")&&(G=G.split(":")[0]),G.endsWith("/USDT")||(G=G.replace("USDT","")+"/USDT");const q=j.map(he=>he.id!==k?he:(he.symbols||[]).includes(G)?(J("已在列表中"),he):{...he,symbols:[...he.symbols||[],G]});T(q),localStorage.setItem("ikun_listgroups",JSON.stringify(q)),As(""),Os(!1),J("已添加 "+G)}function tp(k,O){const G=j.map(q=>q.id!==k?q:{...q,symbols:(q.symbols||[]).filter(he=>he!==O)});T(G),localStorage.setItem("ikun_listgroups",JSON.stringify(G)),J("已移除 "+O)}return r.jsxs(r.Fragment,{children:[r.jsx(rg,{signals:Hu,onDismiss:Vu}),r.jsx(sn,{activePage:i||"monitor",onNavigate:o||(()=>{}),currentUser:e,onOpenLogin:t,onLogout:n}),r.jsx("div",{className:`sidebar-overlay ${h?"show":""}`,onClick:()=>f(!1)}),r.jsxs("div",{className:"app-container",children:[r.jsx(Um,{sidebarOpen:h,timeframe:fe,setTimeframe:Ge,triggerMode:we,setTriggerMode:Wn,watchlistMode:ft,setWatchlistMode:St,alertSettings:D,syncAlertSettings:Qu,setShowEmailBindTip:te,connStatus:Du,customSounds:Fr,setShowStratModal:P}),r.jsxs("div",{className:"main-content",children:[a&&r.jsxs("div",{className:"active-ind-banner",children:[r.jsx("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"#4db8ff",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{flexShrink:0},children:r.jsx("path",{d:"M22 12h-4l-3 9L9 3l-3 9H2"})}),r.jsx("span",{className:"aib-label",children:"当前指标"}),r.jsx("span",{className:"aib-name",children:a.name}),a.author&&r.jsxs("span",{className:"aib-author",children:["by ",a.author]}),r.jsx("button",{className:"aib-close",onClick:s,title:"移除指标",children:r.jsxs("svg",{width:"13",height:"13",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2.5",strokeLinecap:"round",children:[r.jsx("line",{x1:"18",y1:"6",x2:"6",y2:"18"}),r.jsx("line",{x1:"6",y1:"6",x2:"18",y2:"18"})]})})]}),r.jsxs("div",{className:"tabs-header",children:[r.jsxs("button",{className:`tab-btn ${c==="watchlist"?"active":""}`,onClick:()=>{d("watchlist"),St("favorites")},children:[r.jsx("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{marginRight:6,verticalAlign:-3},children:r.jsx("polygon",{points:"12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"})}),"自选列表 (",dn.length,"/20)"]}),j.map(k=>r.jsxs("button",{className:`tab-btn ${c==="group_"+k.id?"active":""}`,onClick:()=>k.url?window.open(k.url,"_blank"):d("group_"+k.id),title:k.url?`打开 ${k.url}`:k.name,children:[k.url?"🌐":"📋"," ",k.name]},k.id)),r.jsxs("button",{className:`tab-btn ${c==="alerts"?"active":""}`,onClick:()=>d("alerts"),children:[r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{marginRight:6,verticalAlign:-3},children:[r.jsx("path",{d:"M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"}),r.jsx("path",{d:"M13.73 21a2 2 0 0 1-3.46 0"})]}),"快讯日志 ",di>0?r.jsx("span",{className:"alert-count-badge",children:di}):null]}),r.jsxs("button",{className:`tab-btn ${c==="io"?"active":""}`,onClick:()=>d("io"),children:[r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{marginRight:6,verticalAlign:-3},children:[r.jsx("line",{x1:"18",y1:"20",x2:"18",y2:"10"}),r.jsx("line",{x1:"12",y1:"20",x2:"12",y2:"4"}),r.jsx("line",{x1:"6",y1:"20",x2:"6",y2:"14"})]}),"OI 持仓异动"]}),r.jsxs("button",{className:`tab-btn ${c==="movers"?"active":""}`,onClick:()=>{d("movers"),St("movers")},children:[r.jsx("span",{style:{marginRight:6,verticalAlign:-1},children:"🔥"}),"今日涨跌榜"]}),j.length<8?r.jsx("button",{title:"添加列表或监控网站",onClick:()=>C(!0),style:{height:28,padding:"0 10px",borderRadius:14,border:"1.5px solid var(--accent)",background:"transparent",color:"var(--accent)",fontSize:18,cursor:"pointer",display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0,transition:"all 0.18s",fontWeight:700,alignSelf:"center",marginLeft:2},onMouseEnter:k=>{k.currentTarget.style.background="var(--accent)",k.currentTarget.style.color="#000"},onMouseLeave:k=>{k.currentTarget.style.background="transparent",k.currentTarget.style.color="var(--accent)"},children:"+"}):r.jsx("span",{style:{fontSize:11,color:"var(--text-secondary)",alignSelf:"center",padding:"0 8px",opacity:.5},children:"列表已满"})]}),c==="watchlist"&&r.jsx(rc,{watchlist:dn,scanData:Rs,watchlistMode:"favorites",exchangeLabel:F.toUpperCase(),searchQ:Ls,setSearchQ:Ps,showDropdown:Ds,setShowDropdown:Is,searchWrapRef:Ms,filteredSymbols:Bs,addSymbol:Us,removeSymbol:Ws}),c==="movers"&&r.jsx(rc,{watchlist:Vn,scanData:Kt,watchlistMode:"movers",exchangeLabel:"Binance",searchQ:Ls,setSearchQ:Ps,showDropdown:Ds,setShowDropdown:Is,searchWrapRef:Ms,filteredSymbols:Bs,addSymbol:Us,removeSymbol:Ws}),j.filter(k=>!k.url).map(k=>c==="group_"+k.id&&r.jsx(Km,{group:k,scanData:Rs,exchangeLabel:$u,groupSearchQ:Iu,setGroupSearchQ:As,groupShowDropdown:Mu,setGroupShowDropdown:Os,groupSearchWrapRef:Au,filteredGroupSymbols:Ou,addSymbolToGroup:ep,removeSymbolFromGroup:tp},k.id)),c==="alerts"&&r.jsx(Vm,{alertLog:Bu,setAlertLog:Fu,unreadCount:di,setUnreadCount:Uu,markAllRead:Wu}),c==="io"&&r.jsx(cg,{})]})]}),w&&r.jsx("div",{className:"modal",onClick:k=>k.target===k.currentTarget&&C(!1),children:r.jsxs("div",{className:"modal-container",style:{width:480,height:"auto",flexDirection:"column",padding:0},children:[r.jsxs("div",{style:{padding:"20px 24px 0",display:"flex",justifyContent:"space-between",alignItems:"center"},children:[r.jsx("div",{style:{fontSize:17,fontWeight:700,color:"var(--text-primary)"},children:"新增"}),r.jsx("button",{onClick:()=>C(!1),style:{background:"none",border:"none",color:"var(--text-secondary)",fontSize:20,cursor:"pointer",lineHeight:1},children:"×"})]}),r.jsx("div",{style:{display:"flex",gap:0,padding:"12px 24px 0",borderBottom:"1px solid var(--border-color)"},children:[["group","📋  新建列表"],["url","🌐  添加网站"]].map(([k,O])=>r.jsx("button",{onClick:()=>B(k),style:{padding:"8px 18px",background:"none",border:"none",cursor:"pointer",fontSize:14,fontWeight:600,color:N===k?"var(--accent)":"var(--text-secondary)",borderBottom:N===k?"2px solid var(--accent)":"2px solid transparent",transition:"all 0.15s",marginBottom:-1},children:O},k))}),r.jsxs("div",{style:{padding:"24px 24px 28px"},children:[N==="group"&&r.jsxs("div",{children:[r.jsx("p",{style:{color:"var(--text-secondary)",fontSize:13,marginBottom:18,lineHeight:1.6},children:"创建一个新的自命名自选列表，可以在不同列表之间快速切换，分组管理你的监控标的。"}),r.jsxs("div",{style:{marginBottom:14},children:[r.jsx("label",{style:{fontSize:13,color:"var(--text-secondary)",display:"block",marginBottom:6},children:"列表名称"}),r.jsx("input",{className:"bn-input",value:K,onChange:k=>le(k.target.value),placeholder:"例如：BTC 主力仓、山寨季观察...",autoFocus:!0,onKeyDown:k=>{if(k.key==="Enter"&&K.trim()){const O={id:Date.now(),name:K.trim(),symbols:[]},G=[...j,O];T(G),localStorage.setItem("ikun_listgroups",JSON.stringify(G)),J(`列表「${O.name}」已创建`),le(""),C(!1)}}})]}),r.jsxs("div",{style:{display:"flex",justifyContent:"flex-end",gap:10},children:[r.jsx("button",{className:"icon-btn",onClick:()=>C(!1),children:"取消"}),r.jsx("button",{className:"icon-btn login-btn",disabled:!K.trim(),onClick:()=>{const k={id:Date.now(),name:K.trim(),symbols:[]},O=[...j,k];T(O),localStorage.setItem("ikun_listgroups",JSON.stringify(O)),J(`列表「${k.name}」已创建`),le(""),C(!1)},children:"创建"})]}),j.filter(k=>!k.url).length>0&&r.jsxs("div",{style:{marginTop:20},children:[r.jsx("div",{style:{fontSize:12,color:"var(--text-secondary)",marginBottom:10,letterSpacing:"0.04em",textTransform:"uppercase"},children:"已有自选列表"}),j.filter(k=>!k.url).map(k=>r.jsxs("div",{style:{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 14px",background:"var(--hover-bg)",border:"1px solid var(--border-color)",borderRadius:8,marginBottom:8,transition:"border-color 0.15s"},onMouseEnter:O=>O.currentTarget.style.borderColor="var(--accent)",onMouseLeave:O=>O.currentTarget.style.borderColor="var(--border-color)",children:[r.jsxs("div",{style:{display:"flex",alignItems:"center",gap:12},children:[r.jsx("div",{style:{width:34,height:34,borderRadius:8,flexShrink:0,background:"rgba(77,184,255,0.1)",border:"1px solid rgba(77,184,255,0.18)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:16},children:"📋"}),r.jsxs("div",{children:[r.jsx("div",{style:{fontSize:14,fontWeight:700,color:"var(--text-primary)",fontFamily:"'Rajdhani',sans-serif"},children:k.name}),r.jsxs("div",{style:{fontSize:11,color:"var(--text-secondary)",marginTop:1},children:[(k.symbols||[]).length," 个标的"]})]})]}),r.jsx("button",{onClick:()=>{const O=j.filter(G=>G.id!==k.id);T(O),localStorage.setItem("ikun_listgroups",JSON.stringify(O))},style:{background:"none",border:"none",color:"var(--text-secondary)",cursor:"pointer",fontSize:18,lineHeight:1,padding:"4px 6px",borderRadius:4,transition:"color 0.15s"},onMouseEnter:O=>O.currentTarget.style.color="#f6465d",onMouseLeave:O=>O.currentTarget.style.color="var(--text-secondary)",title:"删除列表",children:"×"})]},k.id))]})]}),N==="url"&&r.jsxs("div",{children:[r.jsx("p",{style:{color:"var(--text-secondary)",fontSize:13,marginBottom:18,lineHeight:1.6},children:"添加外部监控网站（如 TradingView、CoinGlass、Coingecko 等），可在监控台侧边快速跳转。"}),r.jsxs("div",{style:{marginBottom:14},children:[r.jsx("label",{style:{fontSize:13,color:"var(--text-secondary)",display:"block",marginBottom:6},children:"网站 URL"}),r.jsx("input",{className:"bn-input",value:Q,onChange:k=>ye(k.target.value),placeholder:"https://www.tradingview.com/..."})]}),r.jsxs("div",{style:{marginBottom:20},children:[r.jsx("label",{style:{fontSize:13,color:"var(--text-secondary)",display:"block",marginBottom:6},children:"显示名称（可选）"}),r.jsx("input",{className:"bn-input",value:pe,onChange:k=>I(k.target.value),placeholder:"TradingView 行情"})]}),r.jsxs("div",{style:{display:"flex",justifyContent:"flex-end",gap:10},children:[r.jsx("button",{className:"icon-btn",onClick:()=>C(!1),children:"取消"}),r.jsx("button",{className:"icon-btn login-btn",disabled:!Q.trim(),onClick:()=>{const k=pe.trim()||Q.replace(/^https?:\/\//,"").split("/")[0],O={id:Date.now(),name:k,url:Q.trim(),symbols:[]},G=[...j,O];T(G),localStorage.setItem("ikun_listgroups",JSON.stringify(G)),J(`已添加：${k}`),ye(""),I(""),C(!1)},children:"添加"})]}),j.filter(k=>k.url).length>0&&r.jsxs("div",{style:{marginTop:20},children:[r.jsx("div",{style:{fontSize:12,color:"var(--text-secondary)",marginBottom:10,letterSpacing:"0.04em",textTransform:"uppercase"},children:"已添加的监控网站"}),j.filter(k=>k.url).map(k=>r.jsxs("div",{style:{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 14px",background:"var(--hover-bg)",border:"1px solid var(--border-color)",borderRadius:8,marginBottom:8,transition:"border-color 0.15s"},onMouseEnter:O=>O.currentTarget.style.borderColor="var(--accent)",onMouseLeave:O=>O.currentTarget.style.borderColor="var(--border-color)",children:[r.jsxs("div",{style:{display:"flex",alignItems:"center",gap:12},children:[r.jsx("div",{style:{width:34,height:34,borderRadius:8,flexShrink:0,background:"rgba(252,213,53,0.08)",border:"1px solid rgba(252,213,53,0.18)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:16},children:"🌐"}),r.jsxs("div",{children:[r.jsx("a",{href:k.url,target:"_blank",rel:"noopener noreferrer",style:{fontSize:14,fontWeight:700,color:"var(--accent)",textDecoration:"none",fontFamily:"'Rajdhani',sans-serif",display:"block"},children:k.name}),r.jsx("div",{style:{fontSize:11,color:"var(--text-secondary)",marginTop:1,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"},children:k.url})]})]}),r.jsx("button",{onClick:()=>{const O=j.filter(G=>G.id!==k.id);T(O),localStorage.setItem("ikun_listgroups",JSON.stringify(O))},style:{background:"none",border:"none",color:"var(--text-secondary)",cursor:"pointer",fontSize:18,lineHeight:1,padding:"4px 6px",borderRadius:4,transition:"color 0.15s"},onMouseEnter:O=>O.currentTarget.style.color="#f6465d",onMouseLeave:O=>O.currentTarget.style.color="var(--text-secondary)",title:"删除",children:"×"})]},k.id))]})]})]})]})}),Y&&r.jsx("div",{className:"modal",onClick:k=>k.target===k.currentTarget&&te(!1),children:r.jsxs("div",{style:{background:"var(--sidebar-bg)",borderRadius:14,border:"1px solid var(--border-color)",padding:"32px 28px 28px",width:420,maxWidth:"92vw",boxShadow:"0 24px 64px rgba(0,0,0,0.55)",position:"relative"},children:[r.jsx("button",{onClick:()=>te(!1),style:{position:"absolute",top:14,right:16,background:"none",border:"none",color:"var(--text-secondary)",fontSize:22,cursor:"pointer",lineHeight:1},children:"×"}),r.jsx("div",{style:{width:48,height:48,borderRadius:12,marginBottom:18,background:"rgba(255,150,0,0.12)",border:"1px solid rgba(255,150,0,0.25)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:24},children:"📧"}),r.jsx("div",{style:{fontFamily:"'Rajdhani', sans-serif",fontSize:18,fontWeight:700,color:"var(--text-primary)",marginBottom:10},children:"需要绑定发件邮箱"}),r.jsxs("div",{style:{fontSize:13,color:"var(--text-secondary)",lineHeight:1.7,marginBottom:24},children:["开启邮件推送前，请先在设置中配置你的 SMTP 发件邮箱。",r.jsx("br",{}),"目前支持 QQ 邮箱、Gmail、163 等主流邮箱。"]}),r.jsxs("div",{style:{display:"flex",gap:10},children:[r.jsx("button",{onClick:()=>te(!1),style:{flex:1,padding:"10px 0",borderRadius:8,background:"none",border:"1px solid var(--border-color)",color:"var(--text-secondary)",cursor:"pointer",fontSize:13,fontFamily:"'Rajdhani', sans-serif",fontWeight:700},children:"稍后再说"}),r.jsx("button",{onClick:()=>{te(!1),y(!0),b("email")},style:{flex:1,padding:"10px 0",borderRadius:8,background:"var(--accent)",border:"none",color:"#030812",cursor:"pointer",fontSize:13,fontFamily:"'Rajdhani', sans-serif",fontWeight:700},children:"前往绑定"})]})]})}),_&&r.jsx("div",{className:"modal",onClick:k=>k.target===k.currentTarget&&P(!1),children:r.jsxs("div",{style:{background:"var(--sidebar-bg)",borderRadius:16,border:"1px solid var(--border-color)",padding:"36px 32px 32px",width:480,maxWidth:"92vw",boxShadow:"0 24px 64px rgba(0,0,0,0.55)",position:"relative"},children:[r.jsx("button",{onClick:()=>P(!1),style:{position:"absolute",top:16,right:16,background:"none",border:"none",color:"var(--text-secondary)",fontSize:22,cursor:"pointer",lineHeight:1},children:"×"}),r.jsx("div",{style:{fontFamily:"'Rajdhani', sans-serif",fontSize:20,fontWeight:700,color:"var(--text-primary)",marginBottom:8},children:"添加更多策略"}),r.jsx("div",{style:{fontSize:13,color:"var(--text-secondary)",marginBottom:28,lineHeight:1.6},children:"你希望从哪里获取策略？"}),r.jsxs("div",{style:{display:"flex",flexDirection:"column",gap:14},children:[r.jsxs("button",{onClick:()=>{P(!1),o&&o("community")},style:{background:"var(--input-bg)",border:"1px solid var(--border-color)",borderRadius:10,padding:"18px 20px",cursor:"pointer",textAlign:"left",transition:"border-color 0.2s, background 0.2s",display:"flex",alignItems:"flex-start",gap:16},onMouseEnter:k=>{k.currentTarget.style.borderColor="var(--accent)",k.currentTarget.style.background="var(--hover-bg)"},onMouseLeave:k=>{k.currentTarget.style.borderColor="var(--border-color)",k.currentTarget.style.background="var(--input-bg)"},children:[r.jsx("div",{style:{width:44,height:44,borderRadius:10,flexShrink:0,background:"rgba(77,184,255,0.12)",border:"1px solid rgba(77,184,255,0.2)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:22},children:"🌐"}),r.jsxs("div",{children:[r.jsx("div",{style:{fontFamily:"'Rajdhani', sans-serif",fontWeight:700,fontSize:15,color:"var(--text-primary)",marginBottom:4},children:"前往社区"}),r.jsx("div",{style:{fontSize:12.5,color:"var(--text-secondary)",lineHeight:1.55},children:"在社区中寻找其他开发者和交易员分享的知名策略，一键订阅即可使用"})]})]}),r.jsxs("button",{onClick:()=>{P(!1),o&&o("indicators")},style:{background:"var(--input-bg)",border:"1px solid var(--border-color)",borderRadius:10,padding:"18px 20px",cursor:"pointer",textAlign:"left",transition:"border-color 0.2s, background 0.2s",display:"flex",alignItems:"flex-start",gap:16},onMouseEnter:k=>{k.currentTarget.style.borderColor="#0ecb81",k.currentTarget.style.background="var(--hover-bg)"},onMouseLeave:k=>{k.currentTarget.style.borderColor="var(--border-color)",k.currentTarget.style.background="var(--input-bg)"},children:[r.jsx("div",{style:{width:44,height:44,borderRadius:10,flexShrink:0,background:"rgba(14,203,129,0.1)",border:"1px solid rgba(14,203,129,0.2)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:22},children:"⚗️"}),r.jsxs("div",{children:[r.jsx("div",{style:{fontFamily:"'Rajdhani', sans-serif",fontWeight:700,fontSize:15,color:"var(--text-primary)",marginBottom:4},children:"自建专属策略"}),r.jsx("div",{style:{fontSize:12.5,color:"var(--text-secondary)",lineHeight:1.55},children:"在指标开发页用 AI 生成 PineScript 代码，打造属于你自己的专属量化策略"})]})]})]})]})}),g&&r.jsx(Xm,{modalTab:v,setModalTab:b,onClose:()=>y(!1),onSave:Yu,cfg:M,setCfg:U,emailTemplate:Ee,setEmailTemplate:Pe,customSounds:Fr,soundName:ln,setSoundName:E,soundFileRef:V,uploadSound:Zu,deleteSound:qu,sendTestEmail:Ju,testEmailLoading:$n,testEmailResult:De}),ui.length>0&&r.jsx("div",{className:"notify-stack",children:ui.map((k,O)=>{const G=k.action==="LONG"?"#0ecb81":k.action==="SHORT"?"#f6465d":"#888",q=k.action==="LONG"?"LONG 做多":k.action==="SHORT"?"SHORT 做空":k.action,he=k.trigger_time_full||k.trigger_time||"";return r.jsxs("div",{className:"notify-card",onClick:()=>{d("alerts"),$s([])},children:[O===0&&r.jsxs("div",{style:{display:"flex",justifyContent:"space-between",marginBottom:4},children:[r.jsxs("span",{style:{fontSize:11,color:"var(--text-secondary)"},children:["信号推送 x",ui.length]}),r.jsx("button",{onClick:et=>{et.stopPropagation(),$s([])},style:{background:"none",border:"none",color:"var(--text-secondary)",cursor:"pointer",fontSize:14},children:"✕"})]}),r.jsxs("div",{className:"nc-sym",children:[k.symbol," · ",k.timeframe||fe]}),r.jsxs("div",{className:"nc-info",children:[k.signal," · ",k.detail]}),r.jsx("div",{className:"nc-action",style:{color:G},children:q}),r.jsx("div",{className:"nc-time",children:he})]},String(O)+k.symbol+k.time)})}),z&&r.jsx("div",{className:"toast-bar",children:z}),!u&&r.jsxs("div",{style:{position:"fixed",bottom:0,left:0,right:0,background:"var(--card-bg)",borderTop:"1px solid var(--border-color)",padding:"14px 24px",display:"flex",justifyContent:"space-between",alignItems:"center",zIndex:9999},children:[r.jsx("span",{style:{fontSize:13,color:"var(--text-secondary)"},children:"🍪 本站使用 localStorage 保存自选列表和快讯记录"}),r.jsx("button",{style:{background:"var(--accent)",color:"#000",border:"none",padding:"8px 20px",borderRadius:4,fontWeight:"bold",cursor:"pointer"},onClick:()=>{localStorage.setItem("ikun_cookie_ok","1"),m(!0)},children:"我知道了"})]}),r.jsxs("div",{className:"mobile-bottom-nav",children:[r.jsxs("button",{className:c==="watchlist"?"active":"",onClick:()=>{d("watchlist"),Fs()},children:[r.jsx("svg",{width:"18",height:"18",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",children:r.jsx("polygon",{points:"12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"})}),"自选"]}),r.jsxs("button",{className:c==="alerts"?"active":"",onClick:()=>d("alerts"),children:[r.jsxs("svg",{width:"18",height:"18",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",children:[r.jsx("path",{d:"M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"}),r.jsx("path",{d:"M13.73 21a2 2 0 0 1-3.46 0"})]}),"快讯"]}),r.jsxs("button",{className:c==="io"?"active":"",onClick:()=>d("io"),children:[r.jsxs("svg",{width:"18",height:"18",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",children:[r.jsx("line",{x1:"18",y1:"20",x2:"18",y2:"10"}),r.jsx("line",{x1:"12",y1:"20",x2:"12",y2:"4"}),r.jsx("line",{x1:"6",y1:"20",x2:"6",y2:"14"})]}),"持仓"]}),r.jsxs("button",{onClick:()=>f(k=>!k),children:[r.jsxs("svg",{width:"18",height:"18",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",children:[r.jsx("circle",{cx:"12",cy:"12",r:"3"}),r.jsx("path",{d:"M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"})]}),"配置"]})]})]})}const ac=`
.sig-page {
  position: fixed; inset: 0;
  display: flex; flex-direction: column;
  background: var(--bg-color, #030812);
  font-family: 'DM Sans', sans-serif;
}
.sig-body {
  flex: 1; overflow-y: auto; padding: 28px 40px;
  min-height: 0;
}
.sig-body::-webkit-scrollbar { width: 4px; }
.sig-body::-webkit-scrollbar-thumb { background: rgba(77,184,255,0.2); border-radius: 4px; }

/* ── 标题行 ── */
.sig-toprow {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 24px;
}
.sig-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.9rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4);
}
.sig-live-dot {
  display: inline-flex; align-items: center; gap: 7px;
  font-size: 0.78rem; font-weight: 700; letter-spacing: 0.06em;
  font-family: 'Rajdhani', sans-serif; text-transform: uppercase;
  color: #0ecb81; background: rgba(14,203,129,0.1);
  border: 1px solid rgba(14,203,129,0.25);
  padding: 4px 14px; border-radius: 20px;
}
.sig-live-dot::before {
  content: '';
  width: 6px; height: 6px; border-radius: 50%;
  background: #0ecb81;
  animation: sig-pulse 2s ease-in-out infinite;
}
@keyframes sig-pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.4;transform:scale(.7)} }

/* ── 2栏布局 ── */
.sig-grid { display: grid; grid-template-columns: 340px 1fr; gap: 20px; margin-bottom: 20px; }
.sig-card {
  background: var(--card-bg, rgba(5,13,30,0.85));
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  border-radius: 12px; padding: 22px;
}
.sig-card-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: 1rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4);
  margin-bottom: 16px;
  display: flex; align-items: center; justify-content: space-between;
}

/* ── 渠道行 ── */
.ch-row {
  display: flex; align-items: center; justify-content: space-between;
  padding: 11px 14px; border-radius: 8px; margin-bottom: 8px;
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  background: var(--input-bg, rgba(3,8,18,0.5));
  cursor: pointer; transition: border-color 0.18s, background 0.18s;
}
.ch-row:hover { border-color: rgba(77,184,255,0.3); background: var(--hover-bg); }
.ch-row.bound { border-color: rgba(14,203,129,0.3); }
.ch-left { display: flex; align-items: center; gap: 12px; }
.ch-icon {
  width: 36px; height: 36px; border-radius: 8px;
  display: flex; align-items: center; justify-content: center;
  font-size: 18px; flex-shrink: 0;
}
.ch-name { font-weight: 700; font-size: 0.9rem; color: var(--text-primary, #e2eaf4); }
.ch-desc { font-size: 11.5px; color: var(--text-secondary, #9ca3af); margin-top: 1px; }
.ch-badge {
  font-size: 11px; font-weight: 700; padding: 3px 10px; border-radius: 12px;
  font-family: 'Rajdhani', sans-serif; letter-spacing: 0.04em; flex-shrink: 0;
}
.ch-badge.on  { background: rgba(14,203,129,0.12); color: #0ecb81; border: 1px solid rgba(14,203,129,0.25); }
.ch-badge.off { background: rgba(246,70,93,0.08);  color: #f6465d; border: 1px solid rgba(246,70,93,0.2);  }

/* ── 信号预览 ── */
.sig-preview {
  background: rgba(0,0,0,0.3); border-radius: 8px;
  padding: 16px 18px;
  font-family: 'Space Mono', monospace;
  font-size: 12px; line-height: 1.9;
  color: #a8d4ff; white-space: pre-wrap;
  border: 1px solid rgba(77,184,255,0.08);
}

/* ── 历史信号表格 ── */
.sig-table-wrap { overflow-x: auto; }
.sig-table {
  width: 100%; border-collapse: collapse;
  font-size: 13.5px;
}
.sig-table th {
  text-align: left; padding: 10px 14px;
  color: var(--text-secondary, #9ca3af);
  font-weight: 500; font-size: 12px; letter-spacing: 0.03em;
  border-bottom: 1px solid var(--border-color, rgba(60,140,220,0.14));
  white-space: nowrap;
}
.sig-table td {
  padding: 12px 14px;
  border-bottom: 1px solid var(--border-color, rgba(60,140,220,0.08));
  color: var(--text-primary, #e2eaf4);
  white-space: nowrap;
}
.sig-table tr:hover td { background: var(--hover-bg, rgba(77,184,255,0.05)); }
.sig-badge {
  display: inline-block; padding: 3px 10px; border-radius: 4px;
  font-size: 12px; font-weight: 700; font-family: 'Rajdhani', sans-serif;
}
.sig-badge.long    { background: rgba(14,203,129,0.12); color: #0ecb81; border: 1px solid rgba(14,203,129,0.25); }
.sig-badge.short   { background: rgba(246,70,93,0.12);  color: #f6465d; border: 1px solid rgba(246,70,93,0.25); }
.sig-badge.neutral { background: rgba(77,184,255,0.12); color: #4db8ff; border: 1px solid rgba(77,184,255,0.25); }

.sig-empty {
  text-align: center; padding: 48px 0;
  color: var(--text-secondary, #9ca3af); font-size: 14px;
}
.sig-refreshing { opacity: 0.5; font-size: 12px; color: var(--text-secondary); }

/* ── 配置弹窗 ── */
.sig-modal-mask {
  position: fixed; inset: 0; z-index: 8000;
  background: rgba(0,0,0,0.6);
  display: flex; align-items: center; justify-content: center;
  animation: sig-fadein 0.18s ease;
}
@keyframes sig-fadein { from{opacity:0} to{opacity:1} }
.sig-modal {
  background: var(--card-bg, rgba(8,18,40,0.98));
  border: 1px solid rgba(77,184,255,0.2);
  border-radius: 14px; padding: 30px 28px;
  width: 100%; max-width: 440px;
  box-shadow: 0 24px 64px rgba(0,0,0,0.6);
  animation: sig-slideup 0.2s ease;
  position: relative;
}
@keyframes sig-slideup { from{transform:translateY(12px);opacity:0} to{transform:translateY(0);opacity:1} }
.sig-modal-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.2rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4); margin-bottom: 6px;
}
.sig-modal-sub { font-size: 12.5px; color: var(--text-secondary, #9ca3af); margin-bottom: 22px; line-height: 1.6; }
.sig-modal-label { font-size: 12px; color: var(--text-secondary); margin-bottom: 6px; font-weight: 600; letter-spacing: 0.03em; }
.sig-modal-input {
  width: 100%; box-sizing: border-box;
  background: var(--input-bg, rgba(3,8,18,0.6));
  border: 1px solid rgba(77,184,255,0.2);
  border-radius: 8px; padding: 10px 14px;
  color: var(--text-primary, #e2eaf4);
  font-size: 13.5px; font-family: 'DM Sans', sans-serif;
  outline: none; margin-bottom: 14px;
  transition: border-color 0.2s;
}
.sig-modal-input:focus { border-color: #4db8ff; }
.sig-modal-actions { display: flex; gap: 10px; margin-top: 6px; }
.sig-modal-cancel {
  flex: 1; padding: 10px 0; border-radius: 8px;
  background: none; border: 1px solid var(--border-color, rgba(60,140,220,0.2));
  color: var(--text-secondary, #9ca3af); cursor: pointer;
  font-family: 'Rajdhani', sans-serif; font-size: 0.95rem; font-weight: 700;
  transition: all 0.15s;
}
.sig-modal-cancel:hover { border-color: rgba(246,70,93,0.4); color: #f6465d; }
.sig-modal-save {
  flex: 1; padding: 10px 0; border-radius: 8px;
  background: #4db8ff; border: none; color: #030812;
  cursor: pointer; font-family: 'Rajdhani', sans-serif;
  font-size: 0.95rem; font-weight: 700; transition: opacity 0.15s;
}
.sig-modal-save:hover { opacity: 0.85; }

@media (max-width: 900px) {
  .sig-grid { grid-template-columns: 1fr; }
  .sig-body { padding: 16px; }
}
`,ug=[{key:"email",label:"邮箱推送",desc:"SMTP 邮件，支持自定义模板",icon:"📧",bg:"rgba(255,150,0,0.15)",configKey:null,settingsHint:"请在「设置 → 邮件配置」中绑定 SMTP"},{key:"webhook",label:"自定义 Webhook",desc:"POST 回调到你的服务器",icon:"🔗",bg:"rgba(14,203,129,0.15)",configKey:"webhook",placeholder:"https://your-server.com/webhook",inputLabel:"Webhook URL"},{key:"tg",label:"Telegram Bot",desc:"通过 Bot 实时推送到频道",icon:"✈️",bg:"rgba(0,136,204,0.15)",configKey:"tg",placeholder:"你的 Telegram Bot Token",inputLabel:"Bot Token",inputLabel2:"Chat ID",placeholder2:"-100xxxxxxxxx"},{key:"discord",label:"Discord Webhook",desc:"推送到 Discord 频道",icon:"💬",bg:"rgba(88,101,242,0.15)",configKey:"discord",placeholder:"https://discord.com/api/webhooks/...",inputLabel:"Webhook URL"}];function pg(e){if(!e)return"neutral";const t=e.toLowerCase();return t.includes("做多")||t.includes("long")||t.includes("金叉")||t.includes("买")?"long":t.includes("做空")||t.includes("short")||t.includes("死叉")||t.includes("卖")?"short":"neutral"}function fg(e){if(!e)return"—";try{const t=new Date(e);return t.toLocaleDateString("zh-CN",{month:"2-digit",day:"2-digit"})+" "+t.toLocaleTimeString("zh-CN",{hour:"2-digit",minute:"2-digit"})}catch{return e}}const mg=(e,t,n)=>`I-KUNANCE 信号快报 | 15m
━━━━━━━━━━━━━━━━━━━━━
【标的】${e||"BTC/USDT"}
【价格】${t||"67,234.12"}
【信号】${n||"趋势确认 → 做多"}
【时间】${new Date().toLocaleString("zh-CN")}
━━━━━━━━━━━━━━━━━━━━━
由 I-KUNANCE 自动推送`;function gg({activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}){var ye,pe;const[a,s]=x.useState([]),[l,c]=x.useState(!0),[d,h]=x.useState(!1),[f,g]=x.useState({}),[y,v]=x.useState(!1),[b,z]=x.useState(null),[p,u]=x.useState({}),[m,j]=x.useState(!1),[T,w]=x.useState(""),C=x.useRef(null);x.useEffect(()=>{const I=document.createElement("style");return I.id="signals-page-css",document.getElementById("signals-page-css")||(I.textContent=ac,document.head.appendChild(I)),_(),P(!0),C.current=setInterval(()=>P(!1),1e4),()=>clearInterval(C.current)},[]);async function _(){const I=localStorage.getItem("ikun_token");try{const R=await(await fetch("/api/get_settings",{headers:I?{"X-Token":I}:{}})).json(),M=R.alertSettings||{};v(!!(R.email&&R.emailPass)),g({webhook:R.webhookUrl||"",tg:{token:R.tgToken||"",chatId:R.tgChatId||""},discord:R.discordUrl||""})}catch{}}async function P(I=!1){I?c(!0):h(!0);const S=localStorage.getItem("ikun_token");try{const M=await(await fetch("/api/signal_history",{headers:S?{"X-Token":S}:{}})).json(),U=Array.isArray(M)?M:M.signals||M.data||[];s(U.slice(0,50))}catch{s(hg)}finally{c(!1),h(!1)}}function N(I){if(I.key==="email"){K("请前往「设置 → 邮件配置」绑定 SMTP 发件邮箱");return}const S=f[I.configKey];I.key==="tg"?u({token:(S==null?void 0:S.token)||"",chatId:(S==null?void 0:S.chatId)||""}):u({url:typeof S=="string"?S:""}),z(I)}async function B(){j(!0);const I=localStorage.getItem("ikun_token");let S={};b.key==="tg"?(S={tgToken:p.token,tgChatId:p.chatId},g(R=>({...R,tg:{token:p.token,chatId:p.chatId}}))):b.key==="webhook"?(S={webhookUrl:p.url},g(R=>({...R,webhook:p.url}))):b.key==="discord"&&(S={discordUrl:p.url},g(R=>({...R,discord:p.url})));try{await fetch("/api/save_settings",{method:"POST",headers:{"Content-Type":"application/json",...I?{"X-Token":I}:{}},body:JSON.stringify(S)}),K("渠道配置已保存")}catch{K("保存失败，请检查网络")}j(!1),z(null)}function K(I){w(I),setTimeout(()=>w(""),3e3)}function le(I){var S,R;return I.key==="email"?y:I.key==="tg"?!!((S=f.tg)!=null&&S.token&&((R=f.tg)!=null&&R.chatId)):I.key==="webhook"?!!f.webhook:I.key==="discord"?!!f.discord:!1}const Q=a[0];return r.jsxs("div",{className:"sig-page",children:[r.jsx("style",{children:ac}),r.jsx(sn,{activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}),r.jsxs("div",{className:"sig-body",children:[r.jsxs("div",{className:"sig-toprow",children:[r.jsx("h1",{className:"sig-title",children:"信号推送"}),r.jsxs("div",{style:{display:"flex",alignItems:"center",gap:12},children:[d&&r.jsx("span",{className:"sig-refreshing",children:"刷新中..."}),r.jsx("span",{className:"sig-live-dot",children:"实时监控"})]})]}),r.jsxs("div",{className:"sig-grid",children:[r.jsxs("div",{children:[r.jsxs("div",{className:"sig-card",style:{marginBottom:16},children:[r.jsx("div",{className:"sig-card-title",children:"推送渠道"}),ug.map(I=>{const S=le(I);return r.jsxs("div",{className:`ch-row ${S?"bound":""}`,onClick:()=>N(I),children:[r.jsxs("div",{className:"ch-left",children:[r.jsx("div",{className:"ch-icon",style:{background:I.bg},children:I.icon}),r.jsxs("div",{children:[r.jsx("div",{className:"ch-name",children:I.label}),r.jsx("div",{className:"ch-desc",children:I.desc})]})]}),r.jsx("span",{className:`ch-badge ${S?"on":"off"}`,children:S?"已绑定":"未绑定"})]},I.key)})]}),r.jsxs("div",{className:"sig-card",children:[r.jsx("div",{className:"sig-card-title",children:"推送内容预览"}),r.jsx("div",{className:"sig-preview",children:mg(Q==null?void 0:Q.symbol,(pe=(ye=Q==null?void 0:Q.price)==null?void 0:ye.toFixed)==null?void 0:pe.call(ye,2),(Q==null?void 0:Q.signal)||(Q==null?void 0:Q.action))})]})]}),r.jsxs("div",{className:"sig-card",children:[r.jsxs("div",{className:"sig-card-title",children:["历史信号记录",r.jsx("button",{onClick:()=>P(!1),style:{background:"none",border:"1px solid var(--border-color)",borderRadius:6,padding:"4px 12px",cursor:"pointer",color:"var(--text-secondary)",fontSize:12,fontFamily:"'Rajdhani', sans-serif",fontWeight:700,transition:"all 0.15s"},onMouseEnter:I=>{I.currentTarget.style.borderColor="#4db8ff",I.currentTarget.style.color="#4db8ff"},onMouseLeave:I=>{I.currentTarget.style.borderColor="var(--border-color)",I.currentTarget.style.color="var(--text-secondary)"},children:"刷新"})]}),l?r.jsx("div",{className:"sig-empty",children:"加载中..."}):a.length===0?r.jsxs("div",{className:"sig-empty",children:["暂无信号记录",r.jsx("br",{}),r.jsx("span",{style:{fontSize:12,marginTop:8,display:"block"},children:"在监控台添加自选标的并启动扫描后，信号将在此显示"})]}):r.jsx("div",{className:"sig-table-wrap",children:r.jsxs("table",{className:"sig-table",children:[r.jsx("thead",{children:r.jsxs("tr",{children:[r.jsx("th",{children:"时间"}),r.jsx("th",{children:"标的"}),r.jsx("th",{children:"价格"}),r.jsx("th",{children:"信号"}),r.jsx("th",{children:"周期"}),r.jsx("th",{children:"交易所"})]})}),r.jsx("tbody",{children:a.map((I,S)=>r.jsxs("tr",{children:[r.jsx("td",{style:{color:"var(--text-secondary)",fontSize:12},children:fg(I.time||I.trigger_time||I.timestamp)}),r.jsx("td",{style:{fontWeight:700,fontFamily:"'Rajdhani', sans-serif"},children:I.symbol}),r.jsx("td",{style:{fontFamily:"'Space Mono', monospace",fontSize:12},children:I.price?Number(I.price).toLocaleString():"—"}),r.jsx("td",{children:r.jsx("span",{className:`sig-badge ${pg(I.signal||I.action)}`,children:I.signal||I.action||"—"})}),r.jsx("td",{style:{color:"var(--text-secondary)",fontSize:12},children:I.timeframe||"15m"}),r.jsx("td",{style:{color:"var(--text-secondary)",fontSize:12},children:I.exchange||"Binance"})]},S))})]})})]})]})]}),b&&r.jsx("div",{className:"sig-modal-mask",onClick:I=>I.target===I.currentTarget&&z(null),children:r.jsxs("div",{className:"sig-modal",children:[r.jsx("button",{onClick:()=>z(null),style:{position:"absolute",top:14,right:16,background:"none",border:"none",color:"var(--text-secondary)",fontSize:22,cursor:"pointer"},children:"×"}),r.jsxs("div",{className:"sig-modal-title",children:["配置 ",b.label]}),r.jsx("div",{className:"sig-modal-sub",children:b.desc}),b.key==="tg"?r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sig-modal-label",children:b.inputLabel}),r.jsx("input",{className:"sig-modal-input",value:p.token||"",onChange:I=>u(S=>({...S,token:I.target.value})),placeholder:b.placeholder}),r.jsx("div",{className:"sig-modal-label",children:b.inputLabel2}),r.jsx("input",{className:"sig-modal-input",value:p.chatId||"",onChange:I=>u(S=>({...S,chatId:I.target.value})),placeholder:b.placeholder2})]}):r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sig-modal-label",children:b.inputLabel}),r.jsx("input",{className:"sig-modal-input",value:p.url||"",onChange:I=>u(S=>({...S,url:I.target.value})),placeholder:b.placeholder,autoFocus:!0})]}),r.jsxs("div",{className:"sig-modal-actions",children:[r.jsx("button",{className:"sig-modal-cancel",onClick:()=>z(null),children:"取消"}),r.jsx("button",{className:"sig-modal-save",onClick:B,disabled:m,children:m?"保存中...":"保存"})]})]})}),T&&r.jsx("div",{style:{position:"fixed",bottom:28,left:"50%",transform:"translateX(-50%)",background:"var(--accent, #4db8ff)",color:"#030812",padding:"10px 24px",borderRadius:8,fontSize:13,fontWeight:700,fontFamily:"'Rajdhani', sans-serif",zIndex:9999,boxShadow:"0 8px 24px rgba(0,0,0,0.3)",animation:"sig-fadein 0.2s ease"},children:T})]})}const hg=[{symbol:"BTC/USDT",price:67234.12,signal:"趋势确认 → 做多",timeframe:"15m",exchange:"Binance",time:new Date(Date.now()-6e4).toISOString()},{symbol:"ETH/USDT",price:3456.78,signal:"趋势确认 → 做空",timeframe:"15m",exchange:"Binance",time:new Date(Date.now()-18e4).toISOString()},{symbol:"SOL/USDT",price:142.34,signal:"趋势演进 → 做多",timeframe:"1h",exchange:"OKX",time:new Date(Date.now()-36e4).toISOString()},{symbol:"BNB/USDT",price:412.55,signal:"趋势确认 → 做空",timeframe:"15m",exchange:"Bybit",time:new Date(Date.now()-72e4).toISOString()},{symbol:"ARB/USDT",price:.872,signal:"趋势演进 → 做多",timeframe:"4h",exchange:"Binance",time:new Date(Date.now()-12e5).toISOString()}],sc=`
.mkt-page {
  position: fixed; inset: 0;
  display: flex; flex-direction: column;
  background: var(--bg-color, #030812);
  font-family: 'DM Sans', sans-serif;
}
.mkt-body {
  flex: 1; overflow-y: auto; padding: 28px 40px;
}
.mkt-body::-webkit-scrollbar { width: 4px; }
.mkt-body::-webkit-scrollbar-thumb { background: rgba(77,184,255,0.2); border-radius: 4px; }

/* ── 标题区 ── */
.mkt-toprow { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 28px; }
.mkt-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.9rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4); margin-bottom: 6px;
}
.mkt-sub { color: var(--text-secondary, #9ca3af); font-size: 0.9rem; line-height: 1.6; max-width: 480px; }
.mkt-save-btn {
  padding: 10px 28px; border-radius: 8px;
  background: var(--accent, #4db8ff); border: none;
  color: #030812; cursor: pointer;
  font-family: 'Rajdhani', sans-serif; font-size: 1rem; font-weight: 700;
  letter-spacing: 0.04em; transition: opacity 0.18s;
  flex-shrink: 0; margin-top: 4px;
}
.mkt-save-btn:hover { opacity: 0.85; }
.mkt-save-btn:disabled { opacity: 0.4; cursor: not-allowed; }
.mkt-save-btn.saved {
  background: rgba(14,203,129,0.15);
  border: 1px solid rgba(14,203,129,0.35);
  color: #0ecb81;
}

/* ── 当前状态条 ── */
.mkt-current-bar {
  display: flex; align-items: center; gap: 10px;
  padding: 12px 18px; border-radius: 10px; margin-bottom: 28px;
  background: rgba(77,184,255,0.06);
  border: 1px solid rgba(77,184,255,0.15);
  font-size: 13.5px; color: var(--text-secondary, #9ca3af);
}
.mkt-current-name {
  font-family: 'Rajdhani', sans-serif; font-weight: 700;
  color: var(--accent, #4db8ff); font-size: 0.95rem;
}

/* ── 分区标题 ── */
.mkt-section {
  font-family: 'Rajdhani', sans-serif; font-size: 0.75rem;
  font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase;
  color: rgba(77,184,255,0.5); margin-bottom: 14px; margin-top: 28px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--border-color, rgba(60,140,220,0.14));
}
.mkt-section:first-of-type { margin-top: 0; }

/* ── 卡片网格 ── */
.mkt-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 14px; margin-bottom: 8px;
}
.mkt-card {
  background: var(--card-bg, rgba(5,13,30,0.85));
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  border-radius: 12px; padding: 20px 18px;
  cursor: pointer; transition: all 0.2s;
  position: relative; overflow: hidden;
}
.mkt-card::before {
  content: ''; position: absolute; inset: 0;
  background: linear-gradient(135deg, rgba(77,184,255,0.04) 0%, transparent 60%);
  opacity: 0; transition: opacity 0.2s;
}
.mkt-card:hover { border-color: rgba(77,184,255,0.3); transform: translateY(-2px); box-shadow: 0 8px 24px rgba(0,0,0,0.25); }
.mkt-card:hover::before { opacity: 1; }
.mkt-card.active {
  border-color: var(--accent, #4db8ff);
  background: linear-gradient(135deg, rgba(77,184,255,0.1) 0%, rgba(13,79,160,0.12) 100%);
  box-shadow: 0 0 0 1px rgba(77,184,255,0.2), 0 8px 24px rgba(77,184,255,0.08);
}
.mkt-card.active::before { opacity: 1; }
.mkt-card.coming {
  opacity: 0.5; cursor: not-allowed;
}
.mkt-card.coming:hover { transform: none; border-color: var(--border-color); }
.mkt-card-icon { font-size: 26px; margin-bottom: 12px; }
.mkt-card-name {
  font-family: 'Rajdhani', sans-serif; font-weight: 700;
  color: var(--text-primary, #e2eaf4); font-size: 1rem; margin-bottom: 5px;
}
.mkt-card-desc { color: var(--text-secondary, #9ca3af); font-size: 12px; line-height: 1.5; }
.mkt-card-badge {
  display: inline-block; margin-top: 10px;
  padding: 2px 10px; border-radius: 10px; font-size: 11px; font-weight: 700;
  font-family: 'Rajdhani', sans-serif; letter-spacing: 0.04em;
}
.mkt-card-badge.active-badge {
  background: rgba(77,184,255,0.15); color: #4db8ff;
  border: 1px solid rgba(77,184,255,0.3);
}
.mkt-card-badge.coming-badge {
  background: rgba(255,255,255,0.05); color: rgba(255,255,255,0.3);
  border: 1px solid rgba(255,255,255,0.08);
}
.mkt-checkmark {
  position: absolute; top: 12px; right: 12px;
  width: 20px; height: 20px; border-radius: 50%;
  background: #4db8ff; display: flex; align-items: center; justify-content: center;
}
.mkt-checkmark svg { color: #030812; }

/* ── Toast ── */
.mkt-toast {
  position: fixed; bottom: 28px; left: 50%; transform: translateX(-50%);
  padding: 10px 24px; border-radius: 8px;
  font-family: 'Rajdhani', sans-serif; font-size: 13px; font-weight: 700;
  z-index: 9999; box-shadow: 0 8px 24px rgba(0,0,0,0.3);
  animation: mkt-fadein 0.2s ease;
}
@keyframes mkt-fadein { from{opacity:0;transform:translateX(-50%) translateY(8px)} to{opacity:1;transform:translateX(-50%) translateY(0)} }

@media (max-width: 768px) {
  .mkt-body { padding: 16px; }
  .mkt-grid { grid-template-columns: 1fr 1fr; }
  .mkt-toprow { flex-direction: column; gap: 16px; }
}
`,io=[{id:"binance",icon:"🔶",name:"Binance",desc:"全球最大交易所，流动性最强",live:!0},{id:"okx",icon:"⚫",name:"OKX",desc:"合约 / 现货全品类，深度优秀",live:!0},{id:"bybit",icon:"🟡",name:"Bybit",desc:"衍生品专业平台，费率低廉",live:!0},{id:"bitget",icon:"🔵",name:"Bitget",desc:"跟单 + 合约，适合跟单用户",live:!0},{id:"gate",icon:"🟢",name:"Gate.io",desc:"小市值币种覆盖最广",live:!0},{id:"kucoin",icon:"🌊",name:"KuCoin",desc:"手续费低，另类标的丰富",live:!0}],ao=[{id:"us_stock",icon:"🗽",name:"美股",desc:"NYSE / NASDAQ 实时行情",live:!1},{id:"hk_stock",icon:"🏯",name:"港股",desc:"HKEX 港交所数据",live:!1},{id:"a_stock",icon:"🐉",name:"A 股",desc:"上交所 / 深交所",live:!1},{id:"commodities",icon:"🛢️",name:"大宗商品",desc:"原油、黄金、白银等",live:!1}];function xg({activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i,currentExchange:a,onExchangeChange:s}){var T,w;const[l,c]=x.useState(a||"binance"),[d,h]=x.useState(a||"binance"),[f,g]=x.useState(!1),[y,v]=x.useState(!1),[b,z]=x.useState(null);x.useEffect(()=>{document.getElementById("mkt-page-css")||(()=>{const _=document.createElement("style");_.id="mkt-page-css",_.textContent=sc,document.head.appendChild(_)})();const C=localStorage.getItem("ikun_token");fetch("/api/get_settings",{headers:C?{"X-Token":C}:{}}).then(_=>_.json()).then(_=>{const P=_.exchangeId||"binance";c(P),h(P)}).catch(()=>{})},[]);const p=d!==l;async function u(){var _;if(!p||f)return;g(!0);const C=localStorage.getItem("ikun_token");try{await fetch("/api/save_settings",{method:"POST",headers:{"Content-Type":"application/json",...C?{"X-Token":C}:{}},body:JSON.stringify({exchangeId:d})}),c(d),v(!0),s&&s(d),m(`已切换到 ${((_=[...io,...ao].find(P=>P.id===d))==null?void 0:_.name)||d}`,!0),setTimeout(()=>v(!1),3e3)}catch{m("保存失败，请检查网络",!1)}g(!1)}function m(C,_){z({msg:C,ok:_}),setTimeout(()=>z(null),3e3)}const j=((T=[...io,...ao].find(C=>C.id===l))==null?void 0:T.name)||l;return r.jsxs("div",{className:"mkt-page",children:[r.jsx("style",{children:sc}),r.jsx(sn,{activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}),r.jsxs("div",{className:"mkt-body",children:[r.jsxs("div",{className:"mkt-toprow",children:[r.jsxs("div",{children:[r.jsx("div",{className:"mkt-title",children:"市场选择"}),r.jsx("div",{className:"mkt-sub",children:"选择你想要监控的交易所，监控台的扫描引擎、IOI 持仓数据将同步切换数据源。"})]}),r.jsx("button",{className:`mkt-save-btn ${y?"saved":""}`,onClick:u,disabled:!p||f,children:f?"保存中...":y?"✓ 已保存":p?"确认切换":"未修改"})]}),r.jsxs("div",{className:"mkt-current-bar",children:[r.jsxs("svg",{width:"14",height:"14",viewBox:"0 0 24 24",fill:"none",stroke:"#4db8ff",strokeWidth:"2.5",strokeLinecap:"round",strokeLinejoin:"round",children:[r.jsx("circle",{cx:"12",cy:"12",r:"10"}),r.jsx("polyline",{points:"12 6 12 12 16 14"})]}),"当前监控数据源：",r.jsx("span",{className:"mkt-current-name",children:j}),p&&r.jsxs("span",{style:{marginLeft:8,fontSize:12,color:"rgba(255,180,0,0.8)"},children:["→ 待切换至 ",(w=[...io,...ao].find(C=>C.id===d))==null?void 0:w.name]})]}),r.jsx("div",{className:"mkt-section",children:"加密货币交易所"}),r.jsx("div",{className:"mkt-grid",children:io.map(C=>r.jsxs("div",{className:`mkt-card ${d===C.id?"active":""}`,onClick:()=>h(C.id),children:[d===C.id&&r.jsx("div",{className:"mkt-checkmark",children:r.jsx("svg",{width:"11",height:"11",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"3",strokeLinecap:"round",strokeLinejoin:"round",children:r.jsx("polyline",{points:"20 6 9 17 4 12"})})}),r.jsx("div",{className:"mkt-card-icon",children:C.icon}),r.jsx("div",{className:"mkt-card-name",children:C.name}),r.jsx("div",{className:"mkt-card-desc",children:C.desc}),d===C.id&&r.jsx("div",{className:"mkt-card-badge active-badge",children:"已选中"})]},C.id))}),r.jsx("div",{className:"mkt-section",children:"传统市场（开发中）"}),r.jsx("div",{className:"mkt-grid",children:ao.map(C=>r.jsxs("div",{className:"mkt-card coming",children:[r.jsx("div",{className:"mkt-card-icon",children:C.icon}),r.jsx("div",{className:"mkt-card-name",children:C.name}),r.jsx("div",{className:"mkt-card-desc",children:C.desc}),r.jsx("div",{className:"mkt-card-badge coming-badge",children:"即将上线"})]},C.id))})]}),b&&r.jsx("div",{className:"mkt-toast",style:{background:b.ok?"#0ecb81":"#f6465d",color:"#030812"},children:b.msg})]})}const rr="http://localhost:5000";function lc(){const e=localStorage.getItem("ikun_token")||"";return{"Content-Type":"application/json","X-Token":e,Authorization:`Bearer ${e}`}}const cc=[{id:"views",label:"交易观点",count:326},{id:"market",label:"指标市场",count:562},{id:"premium",label:"付费策略",count:67},{id:"discussion",label:"讨论区",count:801},{id:"news",label:"快讯",count:512}],vg=[{id:"following",label:"我的关注"},{id:"signals",label:"我的信号"}],yg=["最新","热门","精选","关注"],bg=[{name:"#BTC突破",val:"4.2k 讨论"},{name:"#MACD策略",val:"2.8k 讨论"},{name:"#美联储会议",val:"1.5k 讨论"},{name:"#ETH急跌",val:"1.4k 讨论"},{name:"#量化策略",val:"987 讨论"}],kg=["BTC","ETH","SOL","MACD","RSI","布林带","合约","现货","量化","PineScript"],jg=`
/* ── 页面容器：全屏滚动，接主题背景 ── */
.cp-page {
  position: fixed; inset: 0; overflow-y: auto;
  display: flex; flex-direction: column;
  background: var(--bg-color);
  color: var(--text-primary);
  font-family: 'DM Sans', sans-serif;
}

/* ── 三栏主体 ── */
.cp-body {
  display: flex; flex: 1;
  max-width: 1440px; margin: 0 auto; width: 100%;
  padding: 20px 24px 80px; gap: 20px;
  align-items: flex-start;
}

/* ── 左侧边栏 ── */
.cp-left {
  width: 180px; flex-shrink: 0;
  position: sticky; top: 80px;
}
.cp-left-section { margin-bottom: 8px; }
.cp-left-label {
  font-size: 0.72rem; color: var(--text-secondary);
  text-transform: uppercase; letter-spacing: 0.08em;
  font-weight: 700; padding: 16px 12px 8px;
}
.cp-left-item {
  width: 100%; display: flex; align-items: center;
  justify-content: space-between;
  padding: 9px 14px; border-radius: 8px;
  cursor: pointer; border: none; background: none;
  color: var(--text-secondary); font-size: 0.88rem;
  font-weight: 600; transition: all 0.15s; text-align: left;
}
.cp-left-item:hover { background: var(--hover-bg); color: var(--text-primary); }
.cp-left-item { position: relative; }
.cp-left-item.active {
  background: var(--hover-bg); color: var(--accent);
}
.cp-left-item.active::before {
  content: '';
  position: absolute;
  left: 0; top: 50%;
  transform: translateY(-50%);
  width: 3px; height: 60%;
  background: var(--accent);
  border-radius: 0 3px 3px 0;
}
.cp-left-count {
  font-size: 0.72rem; opacity: 0.55; font-weight: 400;
  background: var(--border-color); padding: 1px 6px;
  border-radius: 10px;
}

/* ── 中间主内容 ── */
.cp-main { flex: 1; min-width: 0; }

/* 中间顶栏 */
.cp-main-top {
  display: flex; align-items: center;
  justify-content: space-between; margin-bottom: 16px;
}
.cp-main-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.3rem; font-weight: 700;
  color: var(--text-primary);
}
.cp-tabs { display: flex; gap: 6px; }
.cp-tab {
  padding: 5px 14px; border-radius: 20px;
  background: transparent;
  border: 1px solid var(--border-color);
  color: var(--text-secondary); font-size: 0.82rem;
  font-weight: 600; cursor: pointer; transition: all 0.18s;
}
.cp-tab:hover { color: var(--text-primary); border-color: var(--accent); }
.cp-tab.active { background: var(--accent); color: #fff; border-color: var(--accent); }

/* 搜索/发布栏 */
.cp-compose {
  display: flex; align-items: center; gap: 10px;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 10px; padding: 10px 14px; margin-bottom: 16px;
  transition: border-color 0.2s;
}
.cp-compose:focus-within { border-color: var(--accent); }
.cp-compose-avatar {
  width: 34px; height: 34px; border-radius: 50%;
  background: color-mix(in srgb, var(--accent) 15%, transparent);
  display: flex; align-items: center; justify-content: center;
  color: var(--accent); font-weight: 700; font-size: 0.85rem;
  flex-shrink: 0;
}
.cp-compose-input {
  flex: 1; background: none; border: none; outline: none;
  color: var(--text-primary); font-size: 0.9rem;
  font-family: 'DM Sans', sans-serif;
}
.cp-compose-input::placeholder { color: var(--text-secondary); }
.cp-compose-btn {
  padding: 6px 20px; border-radius: 20px;
  background: var(--accent); color: #fff;
  border: none; font-weight: 700; font-size: 0.85rem;
  cursor: pointer; transition: opacity 0.2s; flex-shrink: 0;
}
.cp-compose-btn:hover { opacity: 0.85; }

/* 帖子卡片 */
.cp-card {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 12px; padding: 18px;
  margin-bottom: 12px;
  transition: border-color 0.2s;
}
.cp-card:hover { border-color: color-mix(in srgb, var(--accent) 30%, transparent); }

.cp-card-header { display: flex; gap: 10px; align-items: flex-start; margin-bottom: 10px; }
.cp-card-avatar {
  width: 40px; height: 40px; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-weight: 800; font-size: 0.85rem;
  flex-shrink: 0; border: 1.5px solid;
}
.cp-card-meta { flex: 1; min-width: 0; }
.cp-card-name-row { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.cp-card-name { font-weight: 700; font-size: 0.93rem; color: var(--text-primary); }
.cp-card-handle { font-size: 0.78rem; color: var(--text-secondary); }
.cp-card-badge {
  padding: 1px 7px; border-radius: 4px; font-size: 0.68rem;
  font-weight: 700; font-family: 'Rajdhani', sans-serif;
  background: color-mix(in srgb, var(--accent) 12%, transparent);
  color: var(--accent); letter-spacing: 0.04em;
}
.cp-card-bull {
  padding: 1px 8px; border-radius: 4px; font-size: 0.72rem; font-weight: 700;
  background: color-mix(in srgb, #0ecb81 12%, transparent); color: #0ecb81;
}
.cp-card-bear {
  padding: 1px 8px; border-radius: 4px; font-size: 0.72rem; font-weight: 700;
  background: color-mix(in srgb, #f6465d 12%, transparent); color: #f6465d;
}
.cp-card-time { font-size: 0.75rem; color: var(--text-secondary); white-space: nowrap; }

.cp-card-body { font-size: 0.9rem; color: var(--text-secondary); line-height: 1.65; margin-bottom: 14px; }
.cp-card-body b, .cp-card-body strong { color: var(--text-primary); }

.cp-card-footer {
  display: flex; gap: 20px; align-items: center;
  border-top: 1px solid var(--border-color); padding-top: 12px;
}
.cp-card-action {
  display: flex; align-items: center; gap: 5px;
  color: var(--text-secondary); font-size: 0.8rem;
  cursor: pointer; background: none; border: none;
  padding: 4px 8px; border-radius: 6px; transition: all 0.15s;
}
.cp-card-action:hover { background: var(--hover-bg); color: var(--text-primary); }
.cp-card-action.liked { color: #f6465d; }
.cp-card-action svg { flex-shrink: 0; }

/* 快讯条目 */
.cp-news-item {
  display: flex; gap: 14px; padding: 16px 0;
  border-bottom: 1px solid var(--border-color);
}
.cp-news-item:last-child { border-bottom: none; }
.cp-news-dot {
  width: 8px; height: 8px; border-radius: 50%;
  margin-top: 6px; flex-shrink: 0;
}
.cp-news-body { flex: 1; min-width: 0; }
.cp-news-src { font-size: 0.72rem; font-weight: 700; color: var(--accent); margin-bottom: 4px; }
.cp-news-title { font-size: 0.9rem; font-weight: 600; color: var(--text-primary); line-height: 1.5; margin-bottom: 4px; }
.cp-news-desc { font-size: 0.8rem; color: var(--text-secondary); line-height: 1.5; }
.cp-news-time { font-size: 0.72rem; color: var(--text-secondary); margin-top: 6px; }

/* ── 右侧边栏 ── */
.cp-right {
  width: 260px; flex-shrink: 0;
  position: sticky; top: 80px;
  display: flex; flex-direction: column; gap: 16px;
}

/* 右侧 Widget */
.cp-widget {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 12px; padding: 16px;
}
.cp-widget-title {
  font-size: 0.88rem; font-weight: 700;
  color: var(--text-primary); margin-bottom: 14px;
  display: flex; align-items: center; justify-content: space-between;
}
.cp-widget-more {
  font-size: 0.72rem; color: var(--accent);
  font-weight: 500; cursor: pointer; opacity: 0.8;
}
.cp-widget-more:hover { opacity: 1; }

/* 行情行 */
.cp-price-row {
  display: flex; align-items: center; gap: 8px;
  padding: 6px 0; border-bottom: 1px solid var(--border-color);
}
.cp-price-row:last-child { border-bottom: none; }
.cp-price-sym { flex: 1; font-weight: 700; font-size: 0.82rem; color: var(--text-primary); }
.cp-price-val { font-family: 'Space Mono', monospace; font-size: 0.8rem; color: var(--text-primary); }
.cp-price-chg { font-family: 'Space Mono', monospace; font-size: 0.78rem; font-weight: 700; min-width: 56px; text-align: right; }
.pos { color: #0ecb81; } .neg { color: #f6465d; }

/* 话题行 */
.cp-topic-row {
  display: flex; align-items: center; gap: 10px;
  padding: 7px 0; cursor: pointer; border-radius: 6px;
  transition: background 0.15s; padding-left: 4px;
}
.cp-topic-row:hover { background: var(--hover-bg); }
.cp-topic-rank { width: 16px; font-size: 0.78rem; font-weight: 700; color: var(--accent); text-align: center; }
.cp-topic-name { flex: 1; font-size: 0.84rem; font-weight: 600; color: var(--text-primary); }
.cp-topic-val { font-size: 0.72rem; color: var(--text-secondary); }

/* 标签云 */
.cp-tag-cloud { display: flex; flex-wrap: wrap; gap: 8px; }
.cp-tag {
  padding: 3px 11px;
  background: color-mix(in srgb, var(--accent) 8%, transparent);
  border: 1px solid color-mix(in srgb, var(--accent) 20%, transparent);
  border-radius: 6px; font-size: 0.75rem;
  color: var(--text-secondary); cursor: pointer; transition: all 0.15s;
}
.cp-tag:hover { color: var(--accent); background: color-mix(in srgb, var(--accent) 14%, transparent); }

/* 加载中 */
.cp-loading { text-align: center; padding: 40px; color: var(--text-secondary); font-size: 0.9rem; }

/* 响应式 */
@media (max-width: 1100px) { .cp-right { display: none; } }
@media (max-width: 760px) { .cp-left { display: none; } }
`;function wg({activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}){const[a,s]=x.useState("views"),[l,c]=x.useState("最新"),[d,h]=x.useState([]),[f,g]=x.useState([]),[y,v]=x.useState([]),[b,z]=x.useState(!0),[p,u]=x.useState(""),m=x.useCallback(async()=>{try{const B=await(await fetch(`${rr}/api/community/posts?page_size=20`)).json();h(B.posts||[])}catch{}},[]),j=x.useCallback(async()=>{try{const B=await(await fetch(`${rr}/api/community/news`)).json();g(B.items||[])}catch{}},[]),T=x.useCallback(async()=>{try{const B=await(await fetch(`${rr}/api/scan?wl=BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT`)).json();v(B.data||[])}catch{}},[]);x.useEffect(()=>{z(!0),Promise.all([m(),j(),T()]).finally(()=>z(!1))},[m,j,T]);async function w(){if(!p.trim())return;if(!n){o==null||o();return}(await(await fetch(`${rr}/api/community/posts`,{method:"POST",headers:lc(),body:JSON.stringify({content:p,tag:"观点"})})).json()).status==="success"&&(u(""),m())}async function C(N){if(!n){o==null||o();return}const K=await(await fetch(`${rr}/api/community/posts/${N}/like`,{method:"POST",headers:lc()})).json();K.status==="success"&&h(le=>le.map(Q=>Q.id===N?{...Q,likes:K.likes,_liked:K.action==="liked"}:Q))}function _(N){const B=Math.floor(Date.now()/1e3-N);return B<60?`${B}秒前`:B<3600?`${Math.floor(B/60)}分钟前`:B<86400?`${Math.floor(B/3600)}小时前`:new Date(N*1e3).toLocaleDateString()}const P=cc.find(N=>N.id===a);return r.jsxs("div",{className:"cp-page",children:[r.jsx("style",{children:jg}),r.jsx(sn,{activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}),r.jsxs("div",{className:"cp-body",children:[r.jsxs("div",{className:"cp-left",children:[r.jsxs("div",{className:"cp-left-section",children:[r.jsx("div",{className:"cp-left-label",children:"探索"}),cc.map(N=>r.jsxs("button",{className:`cp-left-item${a===N.id?" active":""}`,onClick:()=>s(N.id),children:[r.jsx("span",{children:N.label}),r.jsx("span",{className:"cp-left-count",children:N.count})]},N.id))]}),r.jsxs("div",{className:"cp-left-section",children:[r.jsx("div",{className:"cp-left-label",children:"关注"}),vg.map(N=>r.jsx("button",{className:`cp-left-item${a===N.id?" active":""}`,onClick:()=>s(N.id),children:N.label},N.id))]})]}),r.jsxs("div",{className:"cp-main",children:[r.jsxs("div",{className:"cp-main-top",children:[r.jsx("div",{className:"cp-main-title",children:a==="news"?"全球快讯":(P==null?void 0:P.label)||"交易观点"}),a!=="news"&&r.jsx("div",{className:"cp-tabs",children:yg.map(N=>r.jsx("button",{className:`cp-tab${l===N?" active":""}`,onClick:()=>c(N),children:N},N))})]}),a!=="news"&&r.jsxs("div",{className:"cp-compose",children:[r.jsx("div",{className:"cp-compose-avatar",children:n?(n.nickname||n.email)[0].toUpperCase():"?"}),r.jsx("input",{className:"cp-compose-input",placeholder:"分享你的交易观点...",value:p,onChange:N=>u(N.target.value),onKeyDown:N=>N.key==="Enter"&&!N.shiftKey&&w()}),r.jsx("button",{className:"cp-compose-btn",onClick:w,children:"发布"})]}),a!=="news"&&(b?r.jsx("div",{className:"cp-loading",children:"加载中..."}):d.map(N=>{var B,K;return r.jsxs("div",{className:"cp-card",children:[r.jsxs("div",{className:"cp-card-header",children:[r.jsx("div",{className:"cp-card-avatar",style:{background:(N.avatar_color||"#4db8ff")+"22",color:N.avatar_color||"#4db8ff",borderColor:(N.avatar_color||"#4db8ff")+"55"},children:N.avatar||((B=N.author)==null?void 0:B[0])||"?"}),r.jsx("div",{className:"cp-card-meta",children:r.jsxs("div",{className:"cp-card-name-row",children:[r.jsx("span",{className:"cp-card-name",children:N.author}),r.jsxs("span",{className:"cp-card-handle",children:["@",N.author_id||((K=N.author)==null?void 0:K.toLowerCase().replace(/\s/g,"_"))]}),r.jsx("span",{className:"cp-card-badge",children:"BINANCE"}),N.tag==="看空"?r.jsx("span",{className:"cp-card-bear",children:"看空"}):r.jsx("span",{className:"cp-card-bull",children:N.tag||"看多"})]})}),r.jsx("div",{className:"cp-card-time",children:_(N.ts)})]}),r.jsx("div",{className:"cp-card-body",children:N.content}),r.jsxs("div",{className:"cp-card-footer",children:[r.jsxs("button",{className:"cp-card-action",children:[r.jsx("svg",{width:"13",height:"13",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",children:r.jsx("path",{d:"M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"})}),N.comments||0]}),r.jsxs("button",{className:`cp-card-action${N._liked?" liked":""}`,onClick:()=>C(N.id),children:[r.jsx("svg",{width:"13",height:"13",viewBox:"0 0 24 24",fill:N._liked?"currentColor":"none",stroke:"currentColor",strokeWidth:"2",children:r.jsx("path",{d:"M20.84 4.61a5.5 5.5 0 00-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 00-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 000-7.78z"})}),N.likes||0]}),r.jsxs("div",{className:"cp-card-action",children:[r.jsxs("svg",{width:"13",height:"13",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",children:[r.jsx("path",{d:"M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"}),r.jsx("circle",{cx:"12",cy:"12",r:"3"})]}),((N.likes||0)*12+45).toLocaleString()]}),r.jsx("button",{className:"cp-card-action",children:r.jsxs("svg",{width:"13",height:"13",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",children:[r.jsx("circle",{cx:"18",cy:"5",r:"3"}),r.jsx("circle",{cx:"6",cy:"12",r:"3"}),r.jsx("circle",{cx:"18",cy:"19",r:"3"}),r.jsx("line",{x1:"8.59",y1:"13.51",x2:"15.42",y2:"17.49"}),r.jsx("line",{x1:"15.41",y1:"6.51",x2:"8.59",y2:"10.49"})]})})]})]},N.id)})),a==="news"&&(b?r.jsx("div",{className:"cp-loading",children:"加载中..."}):r.jsx("div",{className:"cp-card",children:f.length===0?r.jsx("div",{className:"cp-loading",children:"暂无快讯"}):f.map(N=>r.jsxs("div",{className:"cp-news-item",children:[r.jsx("div",{className:"cp-news-dot",style:{background:N.color||"var(--accent)"}}),r.jsxs("div",{className:"cp-news-body",children:[r.jsx("div",{className:"cp-news-src",children:N.source}),r.jsx("a",{className:"cp-news-title",href:N.url,target:"_blank",rel:"noreferrer",style:{display:"block",textDecoration:"none",color:"inherit"},children:N.title}),N.summary&&r.jsx("div",{className:"cp-news-desc",children:N.summary}),r.jsx("div",{className:"cp-news-time",children:new Date(N.published).toLocaleString()})]})]},N.id))}))]}),r.jsxs("div",{className:"cp-right",children:[r.jsxs("div",{className:"cp-widget",children:[r.jsxs("div",{className:"cp-widget-title",children:["实时行情",r.jsx("span",{className:"cp-widget-more",children:"更多"})]}),y.length>0?y.map(N=>r.jsxs("div",{className:"cp-price-row",children:[r.jsx("span",{className:"cp-price-sym",children:N.symbol.replace("USDT","/USDT")}),r.jsx("span",{className:"cp-price-val",children:Number(N.price||0).toLocaleString()}),r.jsxs("span",{className:`cp-price-chg ${(N.change||0)>=0?"pos":"neg"}`,children:[(N.change||0)>=0?"+":"",Number(N.change||0).toFixed(2),"%"]})]},N.symbol)):["BTC/USDT","ETH/USDT","SOL/USDT","BNB/USDT"].map(N=>r.jsxs("div",{className:"cp-price-row",children:[r.jsx("span",{className:"cp-price-sym",children:N}),r.jsx("span",{style:{color:"var(--text-secondary)",fontSize:"0.75rem"},children:"加载中..."})]},N))]}),r.jsxs("div",{className:"cp-widget",children:[r.jsx("div",{className:"cp-widget-title",children:"热门话题"}),bg.map((N,B)=>r.jsxs("div",{className:"cp-topic-row",children:[r.jsx("span",{className:"cp-topic-rank",children:B+1}),r.jsx("span",{className:"cp-topic-name",children:N.name}),r.jsx("span",{className:"cp-topic-val",children:N.val})]},N.name))]}),r.jsxs("div",{className:"cp-widget",children:[r.jsx("div",{className:"cp-widget-title",children:"热门标签"}),r.jsx("div",{className:"cp-tag-cloud",children:kg.map(N=>r.jsxs("span",{className:"cp-tag",children:["#",N]},N))})]})]})]})]})}const dc=`
/* ── 页面容器 ── */
.ind-page {
  position: fixed; inset: 0;
  display: flex; flex-direction: column;
  background: var(--bg-color, #030812);
  overflow: hidden;
}
.ind-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  padding: 24px 40px 20px;
  gap: 18px;
}
.ind-title-row {
  display: flex; align-items: center; justify-content: space-between;
  flex-shrink: 0;
}
.ind-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.9rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4);
}
.ind-saved-count {
  font-family: 'Rajdhani', sans-serif;
  font-size: 0.8rem; font-weight: 700; letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--accent, #4db8ff);
  background: rgba(77,184,255,0.08);
  border: 1px solid rgba(77,184,255,0.2);
  padding: 4px 14px; border-radius: 20px;
  cursor: pointer; transition: background 0.18s;
}
.ind-saved-count:hover { background: rgba(77,184,255,0.15); }

/* ── 工作区 ── */
.ind-workspace {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  flex: 1;
  overflow: hidden;
  min-height: 0;
}
.ind-card {
  background: var(--card-bg, rgba(5,13,30,0.85));
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  border-radius: 12px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-height: 0;
}
.ind-card iframe { flex: 1; min-height: 0; display: block; width: 100%; height: 100%; }
.ind-card-header {
  padding: 14px 18px;
  border-bottom: 1px solid var(--border-color, rgba(60,140,220,0.14));
  font-weight: 600;
  font-family: 'Rajdhani', sans-serif;
  font-size: 1rem;
  color: var(--text-primary, #e2eaf4);
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}
.ind-header-actions { display: flex; align-items: center; gap: 8px; }

/* ── 聊天区 ── */
.chat-messages {
  flex: 1;
  overflow-y: auto;
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  min-height: 0;
}
.chat-messages::-webkit-scrollbar { width: 4px; }
.chat-messages::-webkit-scrollbar-thumb { background: rgba(77,184,255,0.2); border-radius: 4px; }
.chat-message { max-width: 82%; }
.chat-message.user { margin-left: auto; }
.chat-bubble {
  padding: 11px 15px;
  border-radius: 12px;
  font-size: 13.5px;
  line-height: 1.55;
}
.chat-message.user .chat-bubble {
  background: var(--accent, #4db8ff);
  color: #030812;
  border-radius: 12px 12px 2px 12px;
  font-weight: 500;
}
.chat-message.bot .chat-bubble {
  background: var(--input-bg, rgba(3,8,18,0.6));
  color: var(--text-primary, #e2eaf4);
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  border-radius: 2px 12px 12px 12px;
}
.chat-typing {
  display: flex; gap: 4px; align-items: center; padding: 4px 0;
}
.chat-typing span {
  width: 6px; height: 6px; border-radius: 50%;
  background: rgba(77,184,255,0.5);
  animation: ind-bounce 1.2s infinite;
}
.chat-typing span:nth-child(2) { animation-delay: 0.2s; }
.chat-typing span:nth-child(3) { animation-delay: 0.4s; }
@keyframes ind-bounce {
  0%,60%,100% { transform: translateY(0); opacity: 0.5; }
  30% { transform: translateY(-5px); opacity: 1; }
}

.chat-input-row {
  display: flex;
  gap: 10px;
  padding: 14px 16px;
  border-top: 1px solid var(--border-color, rgba(60,140,220,0.14));
  flex-shrink: 0;
}
.chat-input-row input {
  flex: 1;
  background: var(--input-bg, rgba(3,8,18,0.6));
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  border-radius: 8px;
  color: var(--text-primary, #e2eaf4);
  padding: 9px 14px;
  font-size: 13.5px;
  font-family: 'DM Sans', sans-serif;
  outline: none;
  transition: border-color 0.2s;
}
.chat-input-row input::placeholder { color: var(--text-secondary, #9ca3af); opacity: 0.6; }
.chat-input-row input:focus { border-color: var(--accent, #4db8ff); }
.chat-send-btn {
  padding: 9px 18px;
  background: var(--accent, #4db8ff);
  color: #030812;
  border: none;
  border-radius: 8px;
  font-weight: 700;
  cursor: pointer;
  font-size: 13.5px;
  font-family: 'Rajdhani', sans-serif;
  letter-spacing: 0.04em;
  transition: opacity 0.2s;
  flex-shrink: 0;
}
.chat-send-btn:hover { opacity: 0.85; }
.chat-send-btn:disabled { opacity: 0.4; cursor: not-allowed; }

/* ── 代码区 ── */
.code-area {
  flex: 1;
  padding: 14px 18px;
  font-family: 'Space Mono', monospace;
  font-size: 12.5px;
  line-height: 1.85;
  color: #a0d8ff;
  white-space: pre;
  overflow-y: auto;
  overflow-x: auto;
  background: rgba(0,0,0,0.25);
  min-height: 0;
  resize: none;
  border: none;
  outline: none;
  width: 100%;
  box-sizing: border-box;
  caret-color: #4db8ff;
}
.code-area:focus {
  background: rgba(0,0,0,0.35);
}
.code-area::-webkit-scrollbar { width: 4px; height: 4px; }
.code-area::-webkit-scrollbar-thumb { background: rgba(77,184,255,0.2); border-radius: 4px; }

/* ── 操作按钮 ── */
.ind-action-btn {
  padding: 5px 12px;
  border-radius: 6px;
  font-size: 12px;
  font-family: 'Rajdhani', sans-serif;
  font-weight: 700;
  letter-spacing: 0.04em;
  cursor: pointer;
  transition: all 0.18s;
  white-space: nowrap;
}
.ind-btn-ghost {
  background: transparent;
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  color: var(--text-secondary, #9ca3af);
}
.ind-btn-ghost:hover { border-color: var(--accent, #4db8ff); color: var(--accent, #4db8ff); }
.ind-btn-save {
  background: rgba(14,203,129,0.1);
  border: 1px solid rgba(14,203,129,0.3);
  color: #0ecb81;
}
.ind-btn-save:hover { background: rgba(14,203,129,0.2); border-color: #0ecb81; }
.ind-btn-saved-ok {
  background: rgba(14,203,129,0.15);
  border: 1px solid rgba(14,203,129,0.4);
  color: #0ecb81;
}

/* ── 保存命名弹窗 ── */
.ind-modal-mask {
  position: fixed; inset: 0;
  background: rgba(0,0,0,0.55);
  z-index: 8888;
  display: flex; align-items: center; justify-content: center;
  animation: ind-fadein 0.18s ease;
}
@keyframes ind-fadein { from { opacity: 0; } to { opacity: 1; } }
.ind-modal {
  background: var(--card-bg, rgba(8,18,40,0.98));
  border: 1px solid rgba(77,184,255,0.22);
  border-radius: 14px;
  padding: 32px 36px;
  width: 100%; max-width: 400px;
  box-shadow: 0 24px 64px rgba(0,0,0,0.6);
  animation: ind-slideup 0.2s ease;
}
@keyframes ind-slideup { from { transform: translateY(12px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
.ind-modal-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: 1.2rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4);
  margin-bottom: 18px;
}
.ind-modal-input {
  width: 100%;
  background: var(--input-bg, rgba(3,8,18,0.6));
  border: 1px solid rgba(77,184,255,0.25);
  border-radius: 8px;
  color: var(--text-primary, #e2eaf4);
  padding: 11px 14px;
  font-size: 14px;
  font-family: 'DM Sans', sans-serif;
  outline: none;
  transition: border-color 0.2s;
  box-sizing: border-box;
}
.ind-modal-input:focus { border-color: var(--accent, #4db8ff); }
.ind-modal-actions {
  display: flex; gap: 10px; margin-top: 20px; justify-content: flex-end;
}
.ind-modal-cancel {
  padding: 9px 20px;
  background: transparent;
  border: 1px solid var(--border-color, rgba(60,140,220,0.2));
  border-radius: 7px;
  color: var(--text-secondary, #9ca3af);
  font-family: 'Rajdhani', sans-serif;
  font-size: 0.95rem; font-weight: 700;
  cursor: pointer; transition: all 0.15s;
}
.ind-modal-cancel:hover { border-color: rgba(255,100,100,0.4); color: #f6465d; }
.ind-modal-confirm {
  padding: 9px 24px;
  background: #0ecb81;
  border: none; border-radius: 7px;
  color: #030812;
  font-family: 'Rajdhani', sans-serif;
  font-size: 0.95rem; font-weight: 700;
  cursor: pointer; transition: all 0.15s;
}
.ind-modal-confirm:hover { opacity: 0.85; }

/* ── 已保存指标列表 (TradingView Style) ── */
.ind-saved-section {
  flex-shrink: 0;
  padding: 20px 40px 32px;
  background: var(--bg-color, #030812);
  border-top: 1px solid var(--border-color, rgba(60,140,220,0.14));
}
.tv-list-header-row {
  display: flex; align-items: center; margin-bottom: 12px;
}
.tv-badge {
  display: inline-flex; align-items: center; justify-content: center;
  min-width: 20px; height: 18px; border-radius: 9px; padding: 0 6px;
  background: rgba(77,184,255,0.12); border: 1px solid rgba(77,184,255,0.22);
  color: var(--accent, #4db8ff); font-size: 0.7rem; font-weight: 700; margin-left: 6px;
}
.tv-list-container {
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  border-radius: 10px; overflow: hidden;
  background: var(--card-bg, rgba(5,13,30,0.85));
  box-shadow: 0 2px 16px rgba(0,0,0,0.18);
}
.tv-list-header {
  display: flex; align-items: center; padding: 9px 20px;
  background: rgba(255,255,255,0.025);
  border-bottom: 1px solid var(--border-color, rgba(60,140,220,0.10));
  font-family: 'DM Sans', sans-serif;
  font-size: 0.72rem; font-weight: 700;
  color: var(--text-secondary, #9ca3af);
  text-transform: uppercase; letter-spacing: 0.06em; user-select: none;
}
.tv-list-row {
  display: flex; align-items: center; padding: 11px 20px;
  border-bottom: 1px solid var(--border-color, rgba(60,140,220,0.08));
  cursor: pointer; transition: background 0.13s; position: relative;
}
.tv-list-row:last-child { border-bottom: none; }
.tv-list-row:hover { background: rgba(77,184,255,0.045); }
.tv-list-row.active {
  background: rgba(77,184,255,0.10);
  border-left: 3px solid var(--accent, #4db8ff); padding-left: 17px;
}
.tv-col-name {
  flex: 2 1 0; min-width: 0; overflow: hidden;
  display: flex; align-items: center; gap: 10px;
  font-family: 'Rajdhani', sans-serif; font-size: 0.95rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4);
}
.tv-col-name span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.tv-col-author {
  flex: 1.5 1 0; min-width: 0; overflow: hidden;
  display: flex; align-items: center; gap: 7px;
  font-size: 0.8rem; color: var(--text-secondary, #9ca3af);
}
.tv-col-author span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.tv-avatar {
  width: 22px; height: 22px; border-radius: 50%; flex-shrink: 0;
  background: linear-gradient(135deg, rgba(77,184,255,0.3), rgba(77,184,255,0.1));
  border: 1px solid rgba(77,184,255,0.25);
  display: flex; align-items: center; justify-content: center;
  font-size: 0.68rem; font-weight: 700; color: var(--accent, #4db8ff);
  font-family: 'Rajdhani', sans-serif;
}
.tv-col-pub  { flex: 1 1 0; font-size: 0.78rem; color: var(--text-secondary, #9ca3af); white-space: nowrap; min-width: 0; }
.tv-col-date { flex: 1 1 0; font-size: 0.78rem; color: var(--text-secondary, #9ca3af); white-space: nowrap; min-width: 0; }
.tv-col-type { flex: 1 1 0; min-width: 0; }
.tv-type-badge {
  display: inline-flex; align-items: center;
  padding: 2px 8px; border-radius: 10px;
  font-size: 0.7rem; font-weight: 700; letter-spacing: 0.03em;
}
.tv-type-badge.public { background: rgba(14,203,129,0.1); border: 1px solid rgba(14,203,129,0.25); color: #0ecb81; }
.tv-type-badge.paid   { background: rgba(234,179,8,0.1);  border: 1px solid rgba(234,179,8,0.25);  color: #eab308; }
.tv-col-likes {
  flex: 1 1 0; min-width: 0;
  display: flex; align-items: center; gap: 5px;
  font-size: 0.8rem; color: var(--text-secondary, #9ca3af);
}
.tv-col-actions {
  flex: 1.5 1 0; min-width: 0;
  display: flex; align-items: center; justify-content: flex-end; gap: 6px;
  opacity: 0; transition: opacity 0.15s;
}
.tv-list-row:hover .tv-col-actions { opacity: 1; }
.tv-btn-use {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 5px 12px; border-radius: 6px; border: none; cursor: pointer;
  background: var(--accent, #4db8ff); color: #001a2e;
  font-family: 'Rajdhani', sans-serif; font-weight: 700; font-size: 0.82rem;
  letter-spacing: 0.02em; white-space: nowrap;
  transition: background 0.15s, transform 0.1s, box-shadow 0.15s;
}
.tv-btn-use:hover { background: #74cbff; box-shadow: 0 0 12px rgba(77,184,255,0.35); transform: translateY(-1px); }
.tv-btn-use:active { transform: translateY(0); }
.tv-icon-btn {
  width: 28px; height: 28px; border-radius: 6px;
  display: flex; align-items: center; justify-content: center;
  border: none; background: transparent; color: var(--text-secondary, #9ca3af);
  cursor: pointer; transition: all 0.15s; flex-shrink: 0;
}
.tv-icon-btn:hover { background: rgba(246,70,93,0.15); color: #f6465d; }
@media (max-width: 1100px) { .tv-col-pub { display: none; } }
@media (max-width: 900px) {
  .ind-workspace { grid-template-columns: 1fr; }
  .ind-container { padding: 16px 16px 14px; }
  .ind-saved-section { padding: 14px 16px 20px; }
  .tv-col-author, .tv-col-date { display: none; }
  .tv-col-actions { opacity: 1; }
}
`,Sg=`// IKUNANCE Custom Indicator
// 在左侧描述你的指标，AI 将为你生成代码

//@version=5
indicator("My Indicator", overlay=true)

// 示例：简单移动平均
length = input.int(14, "周期", minval=1)
src    = input.source(close, "数据源")
ma     = ta.sma(src, length)

plot(ma, "SMA", color=color.new(color.blue, 0), linewidth=2)`,Ng=[{role:"bot",text:`你好！我是 IKUNANCE AI 指标生成助手。请用自然语言描述你想要的指标，我会为你生成对应的 PineScript 代码。

例如：「创建一个 RSI 超买超卖指标，超过 70 时发出卖出信号」`}],Pu="ikun_saved_indicators";function Cg(){try{return JSON.parse(localStorage.getItem(Pu)||"[]")}catch{return[]}}function fn(e){try{localStorage.setItem(Pu,JSON.stringify(e))}catch{}}function Tg({activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i,onApplyIndicator:a}){var I;const[s,l]=x.useState(Ng),[c,d]=x.useState(""),[h,f]=x.useState(Sg),[g,y]=x.useState(!1),[v,b]=x.useState(!1),[z,p]=x.useState(Cg),[u,m]=x.useState(null),[j,T]=x.useState(!1),[w,C]=x.useState(""),[_,P]=x.useState(!1),N=x.useRef(null);x.useEffect(()=>{const S="indicators-page-styles";if(!document.getElementById(S)){const R=document.createElement("style");R.id=S,R.textContent=dc,document.head.appendChild(R)}},[]),x.useEffect(()=>{if(!n)return;const S=localStorage.getItem("ikun_token");fetch("/api/indicators",{headers:S?{"X-Token":S}:{}}).then(R=>R.json()).then(R=>{R.indicators&&R.indicators.length>0&&(p(R.indicators),fn(R.indicators))}).catch(()=>{})},[n]),x.useEffect(()=>{var S;(S=N.current)==null||S.scrollIntoView({behavior:"smooth"})},[s,v]),x.useRef([]);function B(){navigator.clipboard.writeText(h).then(()=>{y(!0),setTimeout(()=>y(!1),2e3)})}function K(){T(!0)}async function le(){const S=w.trim()||"未命名指标";T(!1);const R=localStorage.getItem("ikun_token");if(n&&R)try{const U=await(await fetch("/api/indicators/save",{method:"POST",headers:{"Content-Type":"application/json","X-Token":R},body:JSON.stringify({name:S,code:h})})).json();U.indicators&&(p(U.indicators),fn(U.indicators),m(U.indicators.findIndex(D=>D.name===S)))}catch{Q(S)}else Q(S);P(!0),setTimeout(()=>P(!1),3e3)}function Q(S){var U;const R={id:String(Date.now()),name:S,code:h,createdAt:new Date().toISOString(),publishedAt:new Date().toISOString(),type:"public",author:(n==null?void 0:n.username)||((U=n==null?void 0:n.email)==null?void 0:U.split("@")[0])||"我",downloads:0},M=[...z.filter(D=>D.name!==S),R];p(M),fn(M),m(M.findIndex(D=>D.name===S))}function ye(S){f(z[S].code),m(S),P(!1)}async function pe(S,R){S.stopPropagation();const M=z[R],U=localStorage.getItem("ikun_token");if(n&&U&&M.id)try{const F=await(await fetch(`/api/indicators/${M.id}`,{method:"DELETE",headers:{"X-Token":U}})).json();if(F.indicators){p(F.indicators),fn(F.indicators),u===R?m(null):u>R&&m(u-1);return}}catch{}const D=z.filter(($,F)=>F!==R);p(D),fn(D),u===R?m(null):u>R&&m(u-1)}return r.jsxs("div",{className:"ind-page",children:[r.jsx("style",{children:dc}),r.jsx(sn,{activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}),r.jsxs("div",{className:"ind-container",children:[r.jsxs("div",{className:"ind-title-row",children:[r.jsx("h1",{className:"ind-title",children:"AI 指标生成"}),z.length>0&&r.jsxs("span",{className:"ind-saved-count",children:[z.length," 个已保存指标"]})]}),r.jsxs("div",{className:"ind-workspace",children:[r.jsxs("div",{className:"ind-card",style:{position:"relative",overflow:"hidden"},children:[r.jsxs("div",{className:"ind-card-header",style:{flexShrink:0},children:[r.jsxs("span",{style:{display:"flex",alignItems:"center",gap:8},children:[r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",children:[r.jsx("circle",{cx:"12",cy:"12",r:"10"}),r.jsx("path",{d:"M8 14s1.5 2 4 2 4-2 4-2"}),r.jsx("line",{x1:"9",y1:"9",x2:"9.01",y2:"9"}),r.jsx("line",{x1:"15",y1:"9",x2:"15.01",y2:"9"})]}),"豆包 AI"]}),r.jsx("span",{style:{fontSize:11,color:"var(--text-secondary)",fontWeight:400},children:"登录你的豆包账号 · 永久免费"})]}),r.jsx("iframe",{src:"https://www.doubao.com/chat/",title:"豆包 AI",style:{flex:1,border:"none",width:"100%",minHeight:0,display:"block",height:"100%"},allow:"clipboard-read; clipboard-write",sandbox:"allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-storage-access-by-user-activation allow-modals"})]}),r.jsxs("div",{className:"ind-card",children:[r.jsxs("div",{className:"ind-card-header",children:[r.jsxs("span",{style:{display:"flex",alignItems:"center",gap:8},children:["PineScript 代码",_&&r.jsx("span",{style:{fontSize:"0.72rem",fontWeight:700,background:"rgba(14,203,129,0.12)",border:"1px solid rgba(14,203,129,0.3)",color:"#0ecb81",padding:"2px 10px",borderRadius:20,fontFamily:"'Rajdhani', sans-serif",letterSpacing:"0.06em"},children:"已保存"}),u!==null&&!_&&r.jsx("span",{style:{fontSize:"0.72rem",fontWeight:700,background:"rgba(77,184,255,0.08)",border:"1px solid rgba(77,184,255,0.2)",color:"var(--accent,#4db8ff)",padding:"2px 10px",borderRadius:20,fontFamily:"'Rajdhani', sans-serif",letterSpacing:"0.06em"},children:(I=z[u])==null?void 0:I.name})]}),r.jsxs("div",{className:"ind-header-actions",children:[r.jsx("button",{className:`ind-action-btn ${_?"ind-btn-saved-ok":"ind-btn-save"}`,onClick:K,children:_?"✓ 已保存":"保存指标"}),r.jsx("button",{className:"ind-action-btn ind-btn-ghost",onClick:B,children:g?"✓ 已复制":"复制代码"})]})]}),r.jsx("textarea",{className:"code-area",value:h,onChange:S=>f(S.target.value),spellCheck:!1,placeholder:"在左侧豆包 AI 获取 PineScript 代码后，粘贴到这里..."})]})]})]}),z.length>0&&r.jsxs("div",{className:"ind-saved-section",children:[r.jsx("div",{className:"tv-list-header-row",children:r.jsxs("span",{style:{fontFamily:"'Rajdhani', sans-serif",fontWeight:700,fontSize:"0.82rem",letterSpacing:"0.06em",color:"var(--text-secondary)",textTransform:"uppercase"},children:["已保存的指标 ",r.jsx("span",{className:"tv-badge",children:z.length})]})}),r.jsxs("div",{className:"tv-list-container",children:[r.jsxs("div",{className:"tv-list-header",children:[r.jsx("div",{className:"tv-col-name",children:"指标名称"}),r.jsx("div",{className:"tv-col-author",children:"作者"}),r.jsx("div",{className:"tv-col-pub",children:"发布时间"}),r.jsx("div",{className:"tv-col-date",children:"保存时间"}),r.jsx("div",{className:"tv-col-type",children:"类型"}),r.jsx("div",{className:"tv-col-likes",children:"使用数"}),r.jsx("div",{className:"tv-col-actions"})]}),z.map((S,R)=>{const M=S.createdAt?new Date(S.createdAt).toLocaleDateString("zh-CN",{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}):"—",U=S.publishedAt?new Date(S.publishedAt).toLocaleDateString("zh-CN",{month:"short",day:"numeric"}):"—",D=S.author||"我",$=S.downloads??0,F=S.type==="paid";return r.jsxs("div",{className:`tv-list-row ${u===R?"active":""}`,onClick:()=>ye(R),children:[r.jsxs("div",{className:"tv-col-name",children:[r.jsx("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"var(--accent,#4db8ff)",strokeWidth:"2",strokeLinecap:"round",strokeLinejoin:"round",style:{flexShrink:0,opacity:.85},children:r.jsx("path",{d:"M22 12h-4l-3 9L9 3l-3 9H2"})}),r.jsx("span",{title:S.name,children:S.name})]}),r.jsxs("div",{className:"tv-col-author",children:[r.jsx("div",{className:"tv-avatar",children:D.charAt(0).toUpperCase()}),r.jsx("span",{children:D})]}),r.jsx("div",{className:"tv-col-pub",children:U}),r.jsx("div",{className:"tv-col-date",children:M}),r.jsx("div",{className:"tv-col-type",children:r.jsx("span",{className:`tv-type-badge ${F?"paid":"public"}`,children:F?"付费":"公开"})}),r.jsxs("div",{className:"tv-col-likes",children:[r.jsxs("svg",{width:"13",height:"13",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",opacity:"0.5",style:{flexShrink:0},children:[r.jsx("path",{d:"M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"}),r.jsx("circle",{cx:"9",cy:"7",r:"4"}),r.jsx("path",{d:"M23 21v-2a4 4 0 0 0-3-3.87"}),r.jsx("path",{d:"M16 3.13a4 4 0 0 1 0 7.75"})]}),$.toLocaleString()]}),r.jsxs("div",{className:"tv-col-actions",onClick:W=>W.stopPropagation(),children:[r.jsxs("button",{className:"tv-btn-use",onClick:()=>{const W=z.map((Y,te)=>te===R?{...Y,downloads:(Y.downloads??0)+1}:Y);p(W),fn(W),typeof a=="function"&&a(S),typeof t=="function"&&t("monitor")},title:"应用到监控台",children:[r.jsx("svg",{width:"12",height:"12",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2.5",children:r.jsx("polygon",{points:"5 3 19 12 5 21 5 3"})}),"使用指标"]}),r.jsx("button",{className:"tv-icon-btn",onClick:W=>pe(W,R),title:"移除",children:r.jsxs("svg",{width:"13",height:"13",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2.5",strokeLinecap:"round",children:[r.jsx("line",{x1:"18",y1:"6",x2:"6",y2:"18"}),r.jsx("line",{x1:"6",y1:"6",x2:"18",y2:"18"})]})})]})]},S.id)})]})]}),j&&r.jsx("div",{className:"ind-modal-mask",onClick:()=>T(!1),children:r.jsxs("div",{className:"ind-modal",onClick:S=>S.stopPropagation(),children:[r.jsx("div",{className:"ind-modal-title",children:"保存指标"}),r.jsx("input",{className:"ind-modal-input",value:w,onChange:S=>C(S.target.value),onKeyDown:S=>S.key==="Enter"&&le(),placeholder:"给这个指标起个名字...",autoFocus:!0}),r.jsxs("div",{className:"ind-modal-actions",children:[r.jsx("button",{className:"ind-modal-cancel",onClick:()=>T(!1),children:"取消"}),r.jsx("button",{className:"ind-modal-confirm",onClick:le,children:"保存"})]})]})})]})}const Eg="attribute vec2 a;void main(){gl_Position=vec4(a,0,1);}",zg=`
precision highp float;
uniform float t;uniform vec2 r;
float n(vec2 p){return fract(sin(dot(p,vec2(127.1,311.7)))*43758.5453);}
float sn(vec2 p){
  vec2 i=floor(p),f=fract(p),u=f*f*(3.-2.*f);
  return mix(mix(n(i),n(i+vec2(1,0)),u.x),mix(n(i+vec2(0,1)),n(i+vec2(1,1)),u.x),u.y);}
float fbm(vec2 p){float v=0.,a=.5;for(int i=0;i<6;i++){v+=a*sn(p);p=p*2.1+vec2(1.7,9.2);a*=.5;}return v;}
void main(){
  vec2 uv=gl_FragCoord.xy/r;uv.x*=r.x/r.y;
  float tm=t*.05;vec2 p=uv*2.5;
  float v=fbm(p+vec2(tm*.6,tm*.4));
  v+=.5*fbm(p*2.-vec2(tm*.3,tm*.7));
  float c=fract(v*6.);
  float ln=smoothstep(0.,.04,c)*(1.-smoothstep(.06,.12,c));
  float th=smoothstep(0.,.01,c)*(1.-smoothstep(.01,.03,c));
  // Golden theme colors
  vec3 deep=vec3(.14,.09,.02),mid=vec3(.30,.20,.06),acc=vec3(.72,.52,.18),br=vec3(.98,.80,.35);
  vec3 base=mix(deep,mid,v);base=mix(base,acc,v*v*.5);
  vec3 col=mix(base,acc*1.2,ln*.45);
  col=mix(col,br,th*.7);
  col*=1.-length(uv-.5)*1.2*.18;
  gl_FragColor=vec4(col,1.);}
`;function _g(){const e=x.useRef(null);return x.useEffect(()=>{const t=e.current;if(!t)return;const n=t.getContext("webgl");if(!n)return;const o=n.createShader(n.VERTEX_SHADER);n.shaderSource(o,Eg),n.compileShader(o);const i=n.createShader(n.FRAGMENT_SHADER);n.shaderSource(i,zg),n.compileShader(i);const a=n.createProgram();n.attachShader(a,o),n.attachShader(a,i),n.linkProgram(a),n.useProgram(a);const s=n.createBuffer();n.bindBuffer(n.ARRAY_BUFFER,s),n.bufferData(n.ARRAY_BUFFER,new Float32Array([-1,-1,1,-1,-1,1,1,1]),n.STATIC_DRAW);const l=n.getAttribLocation(a,"a");n.enableVertexAttribArray(l),n.vertexAttribPointer(l,2,n.FLOAT,!1,0,0);const c=n.getUniformLocation(a,"t"),d=n.getUniformLocation(a,"r"),h=Date.now();function f(){n.uniform1f(c,(Date.now()-h)/1e3),n.uniform2f(d,t.width,t.height),n.drawArrays(n.TRIANGLE_STRIP,0,4),requestAnimationFrame(f)}function g(){t.width=window.innerWidth*(window.devicePixelRatio||1),t.height=window.innerHeight*(window.devicePixelRatio||1),n.viewport(0,0,t.width,t.height)}g(),window.addEventListener("resize",g),f()},[]),r.jsx("canvas",{ref:e,style:{position:"fixed",inset:0,width:"100%",height:"100%",zIndex:0,opacity:1}})}const Rg=`
@keyframes pp-shimmer {
  0%  { background-position: -200% center }
  100%{ background-position:  200% center }
}
@keyframes pp-scan {
  0%  { left: -80px }
  100%{ left: calc(100% + 80px) }
}
@keyframes pp-float {
  0%,100%{ transform: translateY(0px) }
  50%    { transform: translateY(-6px) }
}
@keyframes pp-glow-pulse {
  0%,100%{ box-shadow: 0 0 36px rgba(77,184,255,0.35), 0 0 72px rgba(41,98,255,0.2) }
  50%    { box-shadow: 0 0 56px rgba(77,184,255,0.55), 0 0 100px rgba(41,98,255,0.35) }
}

.pp-page {
  position: fixed; inset: 0; overflow-y: auto;
  display: flex; flex-direction: column;
  background: transparent;
  font-family: 'DM Sans', sans-serif; color: var(--text-primary, #e2eaf4);
}
.pp-body {
  flex: 1; padding: 60px 24px 80px;
  max-width: 1100px; margin: 0 auto; width: 100%;
}

/* ── Hero ── */
.pp-hero { text-align: center; margin-bottom: 48px; }
.pp-eyebrow {
  display: inline-flex; align-items: center; gap: 8px;
  font-family: 'Rajdhani', sans-serif;
  font-size: 0.75rem; font-weight: 700; letter-spacing: 0.12em;
  text-transform: uppercase; color: #4db8ff;
  background: rgba(77,184,255,0.07);
  border: 1px solid rgba(77,184,255,0.22);
  padding: 4px 16px; border-radius: 100px; margin-bottom: 24px;
}
.pp-main-title {
  font-family: 'Rajdhani', sans-serif;
  font-size: clamp(2.2rem, 5vw, 3.8rem);
  font-weight: 700; line-height: 1.15;
  color: #e8f4ff; margin-bottom: 16px;
}
.pp-main-title .hi {
  background: linear-gradient(100deg,#4db8ff 0%,#90d4ff 40%,#2962ff 70%,#4db8ff 100%);
  background-size: 200% auto;
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  background-clip: text;
  animation: pp-shimmer 5s linear infinite;
}
.pp-sub {
  font-size: 1.05rem; color: rgba(160,200,240,0.6);
  max-width: 520px; margin: 0 auto 32px;
  line-height: 1.7;
}

/* ── 月付/年付切换 ── */
.pp-toggle {
  display: inline-flex; align-items: center; gap: 14px;
  background: rgba(5,13,30,0.7); border: 1px solid rgba(60,140,220,0.18);
  border-radius: 40px; padding: 5px; margin: 0 auto;
}
.pp-toggle-btn {
  padding: 8px 22px; border-radius: 30px; border: none; cursor: pointer;
  font-family: 'Rajdhani', sans-serif; font-size: 0.95rem; font-weight: 700;
  transition: all 0.22s; color: var(--text-secondary, #9ca3af); background: transparent;
}
.pp-toggle-btn.active { background: #2962ff; color: #fff; }
.pp-save-badge {
  font-size: 0.72rem; font-weight: 700; color: #0ecb81;
  background: rgba(14,203,129,0.12); border: 1px solid rgba(14,203,129,0.25);
  padding: 2px 9px; border-radius: 20px; font-family: 'Rajdhani', sans-serif;
  letter-spacing: 0.05em;
}

/* ── 定价网格 ── */
.pp-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 18px; margin-top: 52px; align-items: end;
}

/* ── 普通卡片 ── */
.pp-card {
  background: var(--card-bg, rgba(5,13,30,0.85));
  border: 1px solid var(--border-color, rgba(60,140,220,0.14));
  border-radius: 16px; padding: 28px 24px 24px;
  display: flex; flex-direction: column; gap: 0;
  position: relative; overflow: hidden; transition: transform 0.22s;
}
.pp-card:hover { transform: translateY(-3px); }

/* ── 推荐卡片 ── */
.pp-card.featured {
  border: none; padding: 32px 24px 28px;
  background: linear-gradient(145deg, #0d2a60 0%, #0a1e4a 40%, #071435 100%);
  animation: pp-glow-pulse 3s ease-in-out infinite, pp-float 6s ease-in-out infinite;
  overflow: hidden;
}
.pp-card.featured::before {
  content: '';
  position: absolute; inset: 0;
  border-radius: 16px;
  padding: 1.5px;
  background: linear-gradient(135deg, #4db8ff, #2962ff, #0a7aff, #4db8ff);
  background-size: 300% 300%;
  -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  -webkit-mask-composite: xor; mask-composite: exclude;
  animation: pp-shimmer 4s linear infinite;
  pointer-events: none;
}
.pp-card.featured::after {
  content: '';
  position: absolute; top: 0; left: -80px; width: 80px; height: 100%;
  background: linear-gradient(90deg, transparent, rgba(255,255,255,0.07), transparent);
  animation: pp-scan 3.5s ease-in-out infinite;
  pointer-events: none;
}

.pp-plan-badge {
  position: absolute; top: -1px; left: 50%; transform: translateX(-50%);
  background: linear-gradient(90deg, #2962ff, #4db8ff);
  color: #fff; font-family: 'Rajdhani', sans-serif;
  font-size: 0.72rem; font-weight: 700; letter-spacing: 0.1em;
  padding: 4px 18px; border-radius: 0 0 12px 12px;
  text-transform: uppercase;
}

.pp-plan-name {
  font-family: 'Rajdhani', sans-serif; font-size: 1.05rem;
  font-weight: 700; letter-spacing: 0.06em; color: var(--text-secondary, #9ca3af);
  text-transform: uppercase; margin-bottom: 6px; margin-top: 8px;
}
.pp-card.featured .pp-plan-name { color: rgba(160,210,255,0.7); }

.pp-price-row { display: flex; align-items: baseline; gap: 4px; margin-bottom: 4px; }
.pp-currency { font-size: 1.2rem; font-weight: 700; color: var(--text-primary, #e2eaf4); margin-top: 6px; }
.pp-amount { font-family: 'Rajdhani', sans-serif; font-size: 3rem; font-weight: 800; line-height: 1; color: var(--text-primary, #e2eaf4); }
.pp-card.featured .pp-amount { color: #e8f4ff; }
.pp-period { font-size: 0.85rem; color: var(--text-secondary, #9ca3af); }
.pp-annual-note { font-size: 0.78rem; color: rgba(77,184,255,0.6); margin-bottom: 4px; height: 18px; }
.pp-desc { font-size: 0.87rem; color: var(--text-secondary, #9ca3af); line-height: 1.5; margin-bottom: 18px; min-height: 52px; }

/* 按钮 */
.pp-btn {
  padding: 12px; border-radius: 8px; border: none;
  font-family: 'Rajdhani', sans-serif; font-size: 1rem; font-weight: 700;
  cursor: pointer; transition: all 0.2s; letter-spacing: 0.05em;
  margin-bottom: 20px; text-align: center;
}
.pp-btn-outline {
  border: 1px solid rgba(60,140,220,0.25); background: transparent;
  color: var(--text-primary, #e2eaf4);
}
.pp-btn-outline:hover { border-color: #4db8ff; color: #4db8ff; background: rgba(77,184,255,0.06); }
.pp-btn-featured {
  position: relative; overflow: hidden;
  background: linear-gradient(125deg, #0d4fa0, #1b70cc, #0a7aff);
  background-size: 200% 200%; color: #fff;
  box-shadow: 0 4px 24px rgba(41,98,255,0.45);
  animation: pp-shimmer 4s linear infinite;
}
.pp-btn-featured:hover { transform: translateY(-1px); box-shadow: 0 8px 32px rgba(41,98,255,0.6); }

/* 功能列表 */
.pp-features { display: flex; flex-direction: column; gap: 9px; }
.pp-feature-item {
  display: flex; align-items: flex-start; gap: 8px;
  font-size: 0.87rem; line-height: 1.4;
  color: var(--text-secondary, #9ca3af);
}
.pp-check {
  width: 16px; height: 16px; border-radius: 50%; flex-shrink: 0; margin-top: 1px;
  display: flex; align-items: center; justify-content: center;
  background: rgba(14,203,129,0.15); color: #0ecb81;
}
.pp-check.featured-check { background: rgba(77,184,255,0.15); color: #4db8ff; }
.pp-cross {
  width: 16px; height: 16px; border-radius: 50%; flex-shrink: 0; margin-top: 1px;
  display: flex; align-items: center; justify-content: center;
  background: rgba(100,100,120,0.08); color: rgba(100,100,120,0.4);
}

/* ── 功能对比表 ── */
.pp-compare {
  margin-top: 72px;
}
.pp-compare-title {
  font-family: 'Rajdhani', sans-serif; font-size: 1.8rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4); text-align: center; margin-bottom: 32px;
}
.pp-table {
  width: 100%; border-collapse: collapse;
}
.pp-table th, .pp-table td {
  padding: 13px 16px; text-align: center; font-size: 0.88rem;
  border-bottom: 1px solid var(--border-color, rgba(60,140,220,0.1));
}
.pp-table th {
  font-family: 'Rajdhani', sans-serif; font-weight: 700; font-size: 0.9rem;
  color: var(--text-secondary, #9ca3af); letter-spacing: 0.06em; text-transform: uppercase;
  padding-top: 0;
}
.pp-table th.hl { color: #4db8ff; }
.pp-table td:first-child { text-align: left; color: var(--text-secondary, #9ca3af); }
.pp-table tr:hover td { background: rgba(77,184,255,0.03); }
.pp-tick { color: #0ecb81; font-size: 1rem; }
.pp-cross-icon { color: rgba(100,100,120,0.4); font-size: 0.9rem; }
.pp-td-hl { color: #4db8ff; font-weight: 600; font-family: 'Space Mono', monospace; }

/* ── FAQ ── */
.pp-faq { margin-top: 64px; max-width: 680px; margin-left: auto; margin-right: auto; }
.pp-faq-title {
  font-family: 'Rajdhani', sans-serif; font-size: 1.6rem; font-weight: 700;
  color: var(--text-primary, #e2eaf4); text-align: center; margin-bottom: 28px;
}
.pp-faq-item {
  border-bottom: 1px solid var(--border-color, rgba(60,140,220,0.12));
  padding: 16px 0; cursor: pointer;
}
.pp-faq-q {
  font-size: 0.95rem; font-weight: 600; color: var(--text-primary, #e2eaf4);
  display: flex; justify-content: space-between; align-items: center; gap: 12px;
}
.pp-faq-q svg { flex-shrink: 0; color: rgba(77,184,255,0.5); transition: transform 0.2s; }
.pp-faq-q.open svg { transform: rotate(180deg); }
.pp-faq-a {
  font-size: 0.88rem; color: var(--text-secondary, #9ca3af);
  line-height: 1.7; margin-top: 10px; padding-right: 24px;
}

@media (max-width: 900px) {
  .pp-grid { grid-template-columns: repeat(2, 1fr); }
  .pp-table { font-size: 0.78rem; }
}
@media (max-width: 600px) {
  .pp-grid { grid-template-columns: 1fr; }
}
`,Lg=[{id:"essential",name:"Essential",featured:!1,price:{month:5,year:4},desc:"适合初学者，体验平台核心监控能力，支持3个标的实时监控。",btnText:"Get Started",features:[{text:"3 个自选标的",on:!0},{text:"邮件信号推送",on:!0},{text:"MACD / RSI 指标",on:!0},{text:"社区基础访问",on:!0},{text:"Telegram 推送",on:!1},{text:"AI 指标生成",on:!1},{text:"API 访问",on:!1}]},{id:"plus",name:"Plus",featured:!1,price:{month:15,year:12},desc:"无限标的监控，多渠道推送，适合认真的交易者。",btnText:"Get Plus",features:[{text:"无限自选标的",on:!0},{text:"邮件信号推送",on:!0},{text:"全部内置指标",on:!0},{text:"社区完整访问",on:!0},{text:"Telegram 推送",on:!0},{text:"AI 指标生成",on:!1},{text:"API 访问",on:!1}]},{id:"premium",name:"Premium",featured:!0,price:{month:36,year:29},desc:"AI 自动生成交易策略，多交易所并行监控，最受欢迎的方案。",btnText:"Get Premium",badge:"最受欢迎",features:[{text:"无限自选标的",on:!0},{text:"邮件 + Telegram + Discord",on:!0},{text:"全部内置指标",on:!0},{text:"社区付费区访问",on:!0},{text:"AI 指标生成",on:!0},{text:"多交易所并行",on:!0},{text:"API 访问",on:!1}]},{id:"ultimate",name:"Ultimate",featured:!1,price:{month:90,year:72},desc:"机构级方案，API 直连，私有部署支持，专属客服。",btnText:"Contact Sales",features:[{text:"无限自选标的",on:!0},{text:"全渠道推送",on:!0},{text:"自定义指标开发",on:!0},{text:"社区全功能",on:!0},{text:"AI 指标生成 (无限)",on:!0},{text:"REST + WebSocket API",on:!0},{text:"私有部署支持",on:!0}]}],Pg=[{label:"自选标的数",vals:["3 个","无限","无限","无限"]},{label:"信号延迟",vals:["<5s","<2s","<1s","实时"]},{label:"邮件推送",vals:[!0,!0,!0,!0]},{label:"Telegram 推送",vals:[!1,!0,!0,!0]},{label:"Discord 推送",vals:[!1,!1,!0,!0]},{label:"自定义 Webhook",vals:[!1,!1,!0,!0]},{label:"AI 指标生成",vals:[!1,!1,!0,!0]},{label:"社区付费区",vals:[!1,!1,!0,!0]},{label:"REST API",vals:[!1,!1,!1,!0]},{label:"私有部署",vals:[!1,!1,!1,!0]},{label:"专属客服",vals:[!1,!1,!1,!0]}],Dg=[{q:"可以随时取消订阅吗？",a:"可以。取消后当前周期仍然有效，到期不续费即可。无任何手续费或违约金。"},{q:"支持哪些支付方式？",a:"支持 Visa、Mastercard、支付宝、微信支付，以及 USDC/USDT 加密货币支付。"},{q:"Premium 和 Ultimate 有什么区别？",a:"Premium 面向个人进阶交易者，提供 AI 策略生成和多交易所监控；Ultimate 面向机构和量化团队，额外提供 API 接入、私有部署支持和专属客服。"},{q:"年付省多少？",a:"年付方案约节省 20% 费用，相当于免费使用约 2.4 个月。"},{q:"是否提供免费试用？",a:"注册后可免费使用 Essential 方案 14 天，无需绑定信用卡。"}];function Ig({featured:e}){return r.jsx("span",{className:`pp-check ${e?"featured-check":""}`,children:r.jsx("svg",{width:"10",height:"10",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"3",strokeLinecap:"round",strokeLinejoin:"round",children:r.jsx("polyline",{points:"20 6 9 17 4 12"})})})}function Mg(){return r.jsx("span",{className:"pp-cross",children:r.jsxs("svg",{width:"9",height:"9",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2.5",strokeLinecap:"round",strokeLinejoin:"round",children:[r.jsx("line",{x1:"18",y1:"6",x2:"6",y2:"18"}),r.jsx("line",{x1:"6",y1:"6",x2:"18",y2:"18"})]})})}function Ag({activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}){const[a,s]=x.useState(!1),[l,c]=x.useState(null);return r.jsxs("div",{className:"pp-page",children:[r.jsx("style",{children:Rg}),r.jsx(sn,{activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}),r.jsx(_g,{}),r.jsx("div",{style:{position:"fixed",inset:0,zIndex:1,background:"linear-gradient(to bottom, rgba(3,8,18,0.3) 0%, rgba(3,8,18,0.85) 100%)",pointerEvents:"none"}}),r.jsxs("div",{className:"pp-body",style:{position:"relative",zIndex:5},children:[r.jsxs("div",{className:"pp-hero",children:[r.jsxs("div",{className:"pp-eyebrow",children:[r.jsx("span",{style:{width:6,height:6,borderRadius:"50%",background:"#ffc107",boxShadow:"0 0 8px #ffc107"}}),"开启深度洞察 谋定而动"]}),r.jsxs("h1",{className:"pp-main-title",children:["升级专业版",r.jsx("br",{}),r.jsx("span",{className:"hi",style:{background:"linear-gradient(100deg, #ffc107, #ff9800, #ffeb3b, #ffc107)",backgroundSize:"200% auto",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent",animation:"pp-shimmer 5s linear infinite"},children:"解锁全球顶级深度信号"})]}),r.jsx("p",{className:"pp-sub",children:"超越普通交易者，利用 AI 引擎实时监控全球顶级交易所，获取毫秒级 Alpha 洞察。"}),r.jsxs("div",{style:{display:"flex",justifyContent:"center",alignItems:"center",gap:12},children:[r.jsxs("div",{className:"pp-toggle",children:[r.jsx("button",{className:`pp-toggle-btn ${a?"":"active"}`,onClick:()=>s(!1),children:"月付"}),r.jsx("button",{className:`pp-toggle-btn ${a?"active":""}`,onClick:()=>s(!0),children:"年付"})]}),a&&r.jsx("span",{className:"pp-save-badge",children:"省 20%"})]})]}),r.jsx("div",{className:"pp-grid",children:Lg.map(d=>r.jsxs("div",{className:`pp-card ${d.featured?"featured":""}`,children:[d.badge&&r.jsx("div",{className:"pp-plan-badge",children:d.badge}),r.jsx("div",{className:"pp-plan-name",children:d.name}),r.jsxs("div",{className:"pp-price-row",children:[r.jsx("span",{className:"pp-currency",children:"$"}),r.jsx("span",{className:"pp-amount",children:a?d.price.year:d.price.month}),r.jsx("span",{className:"pp-period",children:"/月"})]}),r.jsx("div",{className:"pp-annual-note",children:a?`$${d.price.year*12}/年，一次性计费`:" "}),r.jsx("div",{className:"pp-desc",children:d.desc}),r.jsx("button",{className:`pp-btn ${d.featured?"pp-btn-featured":"pp-btn-outline"}`,onClick:()=>!n&&(o==null?void 0:o()),children:d.btnText}),r.jsx("div",{className:"pp-features",children:d.features.map((h,f)=>r.jsxs("div",{className:"pp-feature-item",children:[h.on?r.jsx(Ig,{featured:d.featured}):r.jsx(Mg,{}),r.jsx("span",{style:{opacity:h.on?1:.45},children:h.text})]},f))})]},d.id))}),r.jsxs("div",{className:"pp-compare",children:[r.jsx("div",{className:"pp-compare-title",children:"功能详细对比"}),r.jsxs("table",{className:"pp-table",children:[r.jsx("thead",{children:r.jsxs("tr",{children:[r.jsx("th",{style:{textAlign:"left"},children:"功能"}),r.jsx("th",{children:"Essential"}),r.jsx("th",{children:"Plus"}),r.jsx("th",{className:"hl",children:"Premium"}),r.jsx("th",{children:"Ultimate"})]})}),r.jsx("tbody",{children:Pg.map((d,h)=>r.jsxs("tr",{children:[r.jsx("td",{children:d.label}),d.vals.map((f,g)=>r.jsx("td",{className:g===2?"pp-td-hl":"",children:typeof f=="boolean"?f?r.jsx("span",{className:"pp-tick",children:"✓"}):r.jsx("span",{className:"pp-cross-icon",children:"—"}):f},g))]},h))})]})]}),r.jsxs("div",{className:"pp-faq",children:[r.jsx("div",{className:"pp-faq-title",children:"常见问题"}),Dg.map((d,h)=>r.jsxs("div",{className:"pp-faq-item",onClick:()=>c(l===h?null:h),children:[r.jsxs("div",{className:`pp-faq-q ${l===h?"open":""}`,children:[r.jsx("span",{children:d.q}),r.jsx("svg",{width:"14",height:"14",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2.5",strokeLinecap:"round",strokeLinejoin:"round",children:r.jsx("polyline",{points:"6 9 12 15 18 9"})})]}),l===h&&r.jsx("div",{className:"pp-faq-a",children:d.a})]},h))]}),r.jsxs("div",{style:{marginTop:80,padding:"52px 40px",background:"linear-gradient(135deg, rgba(13,42,96,0.6) 0%, rgba(7,20,53,0.8) 100%)",border:"1px solid rgba(77,184,255,0.18)",borderRadius:20,textAlign:"center",position:"relative",overflow:"hidden"},children:[r.jsx("div",{style:{position:"absolute",top:"50%",left:"50%",transform:"translate(-50%,-50%)",width:400,height:200,background:"radial-gradient(ellipse, rgba(41,98,255,0.18) 0%, transparent 70%)",pointerEvents:"none"}}),r.jsx("div",{style:{fontFamily:"'Rajdhani', sans-serif",fontSize:"0.78rem",fontWeight:700,letterSpacing:"0.12em",textTransform:"uppercase",color:"#4db8ff",marginBottom:16},children:"开始你的旅程"}),r.jsxs("h2",{style:{fontFamily:"'Rajdhani', sans-serif",fontSize:"clamp(1.8rem, 3.5vw, 2.8rem)",fontWeight:700,color:"#e8f4ff",marginBottom:12,lineHeight:1.2},children:["14 天免费体验",r.jsx("br",{}),r.jsx("span",{style:{background:"linear-gradient(100deg,#ffc107,#ff9800,#ffeb3b)",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent",backgroundClip:"text"},children:"全功能开放 · 深度定制指标"})]}),r.jsx("p",{style:{fontSize:"0.95rem",color:"rgba(160,200,240,0.6)",marginBottom:32,maxWidth:440,margin:"0 auto 32px"},children:"注册即可免费使用 Essential 全部功能，随时升级，随时取消。"}),r.jsxs("div",{style:{display:"flex",gap:14,justifyContent:"center",flexWrap:"wrap"},children:[r.jsx("button",{onClick:()=>!n&&(o==null?void 0:o()),style:{padding:"13px 36px",background:"linear-gradient(125deg, #0d4fa0, #1b70cc, #0a7aff)",backgroundSize:"200% 200%",color:"#fff",border:"none",borderRadius:8,fontFamily:"'Rajdhani', sans-serif",fontSize:"1.05rem",fontWeight:700,letterSpacing:"0.06em",cursor:"pointer",boxShadow:"0 4px 24px rgba(41,98,255,0.45)",transition:"transform 0.15s, box-shadow 0.15s"},onMouseEnter:d=>{d.currentTarget.style.transform="translateY(-2px)",d.currentTarget.style.boxShadow="0 8px 32px rgba(41,98,255,0.6)"},onMouseLeave:d=>{d.currentTarget.style.transform="",d.currentTarget.style.boxShadow="0 4px 24px rgba(41,98,255,0.45)"},children:"免费开始"}),r.jsx("button",{onClick:()=>t==null?void 0:t("monitor"),style:{padding:"13px 36px",background:"transparent",color:"rgba(160,200,240,0.8)",border:"1px solid rgba(77,184,255,0.22)",borderRadius:8,fontFamily:"'Rajdhani', sans-serif",fontSize:"1.05rem",fontWeight:700,letterSpacing:"0.06em",cursor:"pointer",transition:"all 0.18s"},onMouseEnter:d=>{d.currentTarget.style.borderColor="#4db8ff",d.currentTarget.style.color="#4db8ff"},onMouseLeave:d=>{d.currentTarget.style.borderColor="rgba(77,184,255,0.22)",d.currentTarget.style.color="rgba(160,200,240,0.8)"},children:"先逛逛"})]})]})]})]})}const li="http://localhost:5000";function Ko(){return localStorage.getItem("ikun_token")||""}function ci(){return{"Content-Type":"application/json","X-Token":Ko(),Authorization:`Bearer ${Ko()}`}}async function Og(){try{return await(await fetch(`${li}/api/get_settings`,{headers:ci()})).json()}catch{return{}}}async function Bg(e){try{return await(await fetch(`${li}/api/save_settings`,{method:"POST",headers:ci(),body:JSON.stringify(e)})).json()}catch{return{status:"error",msg:"后端未连接"}}}async function Fg(e){try{return await(await fetch(`${li}/api/test_push`,{method:"POST",headers:ci(),body:JSON.stringify(e)})).json()}catch{return{status:"error",msg:"后端未连接"}}}async function Ug(e,t){try{return await(await fetch(`${li}/api/auth/change_password`,{method:"POST",headers:ci(),body:JSON.stringify({oldPassword:e,newPassword:t})})).json()}catch{return{status:"error",msg:"后端未连接"}}}const Wg=`
.sp-page { position:fixed; inset:0; overflow-y:auto; display:flex; flex-direction:column; background:var(--bg-color,#030812); color:var(--text-primary,#e2eaf4); font-family:'DM Sans',sans-serif; }
.sp-body { display:flex; flex:1; max-width:1100px; margin:0 auto; width:100%; padding:40px 24px 80px; gap:32px; }

.sp-sidebar { width:210px; flex-shrink:0; }
.sp-sidebar-title { font-family:'Rajdhani',sans-serif; font-size:0.72rem; font-weight:700; letter-spacing:0.12em; text-transform:uppercase; color:var(--text-secondary,#9ca3af); padding:0 12px; margin-bottom:8px; margin-top:24px; }
.sp-sidebar-title:first-child { margin-top:0; }
.sp-nav-btn { width:100%; text-align:left; background:none; border:none; padding:9px 12px; border-radius:8px; font-size:0.9rem; color:var(--text-secondary,#9ca3af); cursor:pointer; display:flex; align-items:center; gap:10px; transition:all 0.15s; font-family:'DM Sans',sans-serif; }
.sp-nav-btn:hover { background:var(--hover-bg,rgba(77,184,255,0.07)); color:var(--text-primary,#e2eaf4); }
.sp-nav-btn.active { background:var(--hover-bg,rgba(77,184,255,0.1)); color:var(--accent,#4db8ff); font-weight:600; }
.sp-nav-btn svg { flex-shrink:0; opacity:0.7; }
.sp-nav-btn.active svg { opacity:1; }

.sp-main { flex:1; min-width:0; }
.sp-section-title { font-family:'Rajdhani',sans-serif; font-size:1.5rem; font-weight:700; color:var(--text-primary,#e2eaf4); margin-bottom:4px; }
.sp-section-sub { font-size:0.87rem; color:var(--text-secondary,#9ca3af); margin-bottom:28px; }

.sp-card { background:var(--card-bg,rgba(5,13,30,0.85)); border:1px solid var(--border-color,rgba(60,140,220,0.14)); border-radius:12px; padding:24px; margin-bottom:16px; }
.sp-card-title { font-size:0.95rem; font-weight:600; color:var(--text-primary,#e2eaf4); margin-bottom:4px; }
.sp-card-desc { font-size:0.82rem; color:var(--text-secondary,#9ca3af); margin-bottom:18px; line-height:1.5; }

.sp-row { display:flex; align-items:center; gap:10px; margin-bottom:12px; }
.sp-row:last-child { margin-bottom:0; }
.sp-label { font-size:0.85rem; color:var(--text-secondary,#9ca3af); width:90px; flex-shrink:0; }
.sp-input { flex:1; background:var(--input-bg,rgba(5,13,30,0.6)); border:1px solid var(--border-color,rgba(60,140,220,0.14)); color:var(--text-primary,#e2eaf4); padding:9px 12px; border-radius:8px; font-size:0.9rem; outline:none; transition:border-color 0.2s; font-family:'DM Sans',sans-serif; }
.sp-input:focus { border-color:var(--accent,#4db8ff); }
.sp-input::placeholder { color:var(--text-secondary,#9ca3af); opacity:0.6; }
.sp-input:disabled { opacity:0.4; cursor:not-allowed; }
.sp-select { flex:1; background:var(--input-bg,rgba(5,13,30,0.6)); border:1px solid var(--border-color,rgba(60,140,220,0.14)); color:var(--text-primary,#e2eaf4); padding:9px 12px; border-radius:8px; font-size:0.9rem; outline:none; cursor:pointer; }
.sp-select:focus { border-color:var(--accent,#4db8ff); }

.sp-btn { padding:9px 20px; border-radius:8px; font-size:0.88rem; font-weight:600; cursor:pointer; transition:all 0.18s; font-family:'Rajdhani',sans-serif; letter-spacing:0.04em; border:none; }
.sp-btn:disabled { opacity:0.45; cursor:not-allowed; }
.sp-btn-primary { background:var(--accent,#4db8ff); color:#000; }
.sp-btn-primary:hover:not(:disabled) { opacity:0.88; transform:translateY(-1px); }
.sp-btn-outline { background:transparent; border:1px solid var(--border-color,rgba(60,140,220,0.18)); color:var(--text-secondary,#9ca3af); }
.sp-btn-outline:hover:not(:disabled) { border-color:var(--accent,#4db8ff); color:var(--accent,#4db8ff); }
.sp-btn-danger { background:rgba(246,70,93,0.12); border:1px solid rgba(246,70,93,0.3); color:#F6465D; }
.sp-btn-danger:hover:not(:disabled) { background:rgba(246,70,93,0.2); }
.sp-btn-sm { padding:6px 14px; font-size:0.82rem; }
.sp-btn-test { background:rgba(14,203,129,0.12); border:1px solid rgba(14,203,129,0.3); color:#0ecb81; }
.sp-btn-test:hover:not(:disabled) { background:rgba(14,203,129,0.22); }

.sp-badge { display:inline-flex; align-items:center; gap:5px; padding:3px 10px; border-radius:20px; font-size:0.78rem; font-weight:600; }
.sp-badge-ok { background:rgba(14,203,129,0.12); color:#0ecb81; border:1px solid rgba(14,203,129,0.25); }
.sp-badge-warn { background:rgba(77,184,255,0.12); color:#4db8ff; border:1px solid rgba(77,184,255,0.25); }
.sp-badge-off { background:rgba(120,120,140,0.12); color:#9ca3af; border:1px solid rgba(120,120,140,0.2); }
.sp-dot { width:6px; height:6px; border-radius:50%; background:currentColor; }

.sp-toggle { position:relative; width:42px; height:22px; flex-shrink:0; }
.sp-toggle input { opacity:0; width:0; height:0; }
.sp-toggle-track { position:absolute; inset:0; border-radius:11px; background:var(--border-color,rgba(60,140,220,0.14)); cursor:pointer; transition:background 0.2s; }
.sp-toggle input:checked + .sp-toggle-track { background:var(--accent,#4db8ff); }
.sp-toggle-thumb { position:absolute; top:3px; left:3px; width:16px; height:16px; border-radius:50%; background:#fff; transition:transform 0.2s; pointer-events:none; }
.sp-toggle input:checked ~ .sp-toggle-thumb { transform:translateX(20px); }

.sp-sound-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(140px,1fr)); gap:10px; margin-bottom:16px; }
.sp-sound-card { background:var(--hover-bg,rgba(77,184,255,0.06)); border:1px solid var(--border-color,rgba(60,140,220,0.14)); border-radius:8px; padding:12px; cursor:pointer; transition:all 0.18s; text-align:center; }
.sp-sound-card:hover { border-color:var(--accent,#4db8ff); }
.sp-sound-card.active { border-color:var(--accent,#4db8ff); background:rgba(77,184,255,0.1); }
.sp-sound-icon { font-size:1.4rem; margin-bottom:6px; }
.sp-sound-name { font-size:0.8rem; color:var(--text-primary,#e2eaf4); font-weight:600; }
.sp-sound-sub { font-size:0.72rem; color:var(--text-secondary,#9ca3af); margin-top:2px; }

.sp-upload-zone { border:2px dashed var(--border-color,rgba(60,140,220,0.2)); border-radius:10px; padding:24px; text-align:center; cursor:pointer; transition:border-color 0.2s; }
.sp-upload-zone:hover { border-color:var(--accent,#4db8ff); }
.sp-upload-zone svg { opacity:0.4; margin-bottom:8px; }
.sp-upload-text { font-size:0.85rem; color:var(--text-secondary,#9ca3af); }
.sp-upload-hint { font-size:0.75rem; color:var(--text-secondary,#9ca3af); opacity:0.6; margin-top:4px; }

.sp-model-row { display:flex; align-items:center; gap:12px; padding:12px 0; border-bottom:1px solid var(--border-color,rgba(60,140,220,0.08)); }
.sp-model-row:last-child { border-bottom:none; }
.sp-model-icon { width:32px; height:32px; border-radius:8px; display:flex; align-items:center; justify-content:center; font-size:1rem; flex-shrink:0; }
.sp-model-name { flex:1; font-size:0.9rem; font-weight:600; color:var(--text-primary,#e2eaf4); }
.sp-model-sub { font-size:0.78rem; color:var(--text-secondary,#9ca3af); }

.sp-divider { border:none; border-top:1px solid var(--border-color,rgba(60,140,220,0.1)); margin:20px 0; }

.sp-toast { position:fixed; bottom:28px; left:50%; transform:translateX(-50%); padding:10px 24px; border-radius:8px; font-size:0.9rem; font-weight:700; z-index:9999; pointer-events:none; animation:sp-fadein 0.3s ease; }
.sp-toast-ok  { background:var(--accent,#4db8ff); color:#000; }
.sp-toast-err { background:#F6465D; color:#fff; }
@keyframes sp-fadein { from{opacity:0;transform:translateX(-50%) translateY(10px)} to{opacity:1;transform:translateX(-50%) translateY(0)} }

@media(max-width:700px) {
  .sp-body { flex-direction:column; padding:20px 12px 60px; }
  .sp-sidebar { width:100%; }
}
`,uc=[{id:"notifications",label:"通知渠道",icon:r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.8",strokeLinecap:"round",children:[r.jsx("path",{d:"M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9"}),r.jsx("path",{d:"M13.73 21a2 2 0 01-3.46 0"})]}),group:"推送"},{id:"webhook",label:"Webhook",icon:r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.8",strokeLinecap:"round",children:[r.jsx("path",{d:"M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71"}),r.jsx("path",{d:"M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71"})]}),group:"推送"},{id:"sound",label:"提示音",icon:r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.8",strokeLinecap:"round",children:[r.jsx("polygon",{points:"11 5 6 9 2 9 2 15 6 15 11 19 11 5"}),r.jsx("path",{d:"M19.07 4.93a10 10 0 010 14.14M15.54 8.46a5 5 0 010 7.07"})]}),group:"推送"},{id:"ai",label:"AI 接口",icon:r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.8",strokeLinecap:"round",children:[r.jsx("rect",{x:"3",y:"3",width:"18",height:"18",rx:"3"}),r.jsx("path",{d:"M9 9h.01M15 9h.01M9 15h6"})]}),group:"开发"},{id:"exchange",label:"交易所",icon:r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.8",strokeLinecap:"round",children:[r.jsx("line",{x1:"12",y1:"1",x2:"12",y2:"23"}),r.jsx("path",{d:"M17 5H9.5a3.5 3.5 0 000 7h5a3.5 3.5 0 010 7H6"})]}),group:"开发"},{id:"appearance",label:"外观",icon:r.jsxs("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.8",strokeLinecap:"round",children:[r.jsx("circle",{cx:"12",cy:"12",r:"3"}),r.jsx("path",{d:"M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-2 2 2 2 0 01-2-2v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 01-2-2 2 2 0 012-2h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 012-2 2 2 0 012 2v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 012 2 2 2 0 01-2 2h-.09a1.65 1.65 0 00-1.51 1z"})]}),group:"偏好"},{id:"account",label:"账户安全",icon:r.jsx("svg",{width:"16",height:"16",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.8",strokeLinecap:"round",children:r.jsx("path",{d:"M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"})}),group:"偏好"}],$g=[{id:"chime",name:"水晶钟声",icon:"🔔",desc:"清脆"},{id:"ping",name:"Ping",icon:"🎵",desc:"简洁"},{id:"beep",name:"警报音",icon:"📡",desc:"急促"},{id:"soft",name:"轻柔提示",icon:"🎶",desc:"舒缓"},{id:"none",name:"静音",icon:"🔕",desc:"无声"}],pc=[{id:"openai",name:"OpenAI",icon:"🤖",bg:"rgba(16,163,127,0.15)",placeholder:"sk-..."},{id:"claude",name:"Claude",icon:"🧠",bg:"rgba(207,92,54,0.15)",placeholder:"sk-ant-..."},{id:"deepseek",name:"DeepSeek",icon:"🔍",bg:"rgba(77,184,255,0.15)",placeholder:"sk-..."},{id:"gemini",name:"Gemini",icon:"✨",bg:"rgba(66,133,244,0.15)",placeholder:"AIza..."},{id:"custom",name:"自定义端点",icon:"⚙️",bg:"rgba(160,160,180,0.15)",placeholder:"https://api.example.com/v1"}],Hg=[{id:"binance",name:"Binance",icon:"🟡"},{id:"okx",name:"OKX",icon:"⚫"},{id:"bybit",name:"Bybit",icon:"🟠"},{id:"bitget",name:"Bitget",icon:"🔵"}];function so({checked:e,onChange:t}){return r.jsxs("label",{className:"sp-toggle",children:[r.jsx("input",{type:"checkbox",checked:e,onChange:n=>t(n.target.checked)}),r.jsx("span",{className:"sp-toggle-track"}),r.jsx("span",{className:"sp-toggle-thumb"})]})}function Kg({activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}){const[a,s]=x.useState("notifications"),[l,c]=x.useState({msg:"",type:"ok"}),[d,h]=x.useState(!1),f=x.useRef(),[g,y]=x.useState(""),[v,b]=x.useState(""),[z,p]=x.useState(""),[u,m]=x.useState(!1),[j,T]=x.useState(""),[w,C]=x.useState(!1),[_,P]=x.useState(""),[N,B]=x.useState(!1),[K,le]=x.useState("chime"),[Q,ye]=x.useState([]),[pe,I]=x.useState({}),[S,R]=x.useState({openai:"gpt-4o",claude:"claude-sonnet-4-5",deepseek:"deepseek-chat",gemini:"gemini-1.5-pro",custom:""}),[M,U]=x.useState("openai"),[D,$]=x.useState({binance:{key:"",secret:""},okx:{key:"",secret:""},bybit:{key:"",secret:""},bitget:{key:"",secret:""}}),[F,W]=x.useState("zh"),[Y,te]=x.useState("normal"),[Ee,Pe]=x.useState(!0),[fe,Ge]=x.useState(""),[we,Wn]=x.useState(""),[ft,St]=x.useState("");x.useEffect(()=>{Ko()&&Og().then(E=>{E&&(E.email&&y(E.email),E.tgToken&&b(E.tgToken),E.tgChatId&&p(E.tgChatId),E.discordUrl&&T(E.discordUrl),E.webhookUrl&&P(E.webhookUrl),E.tgToken&&E.tgChatId&&m(!0),E.discordUrl&&C(!0),E.webhookUrl&&B(!0),E.customSounds&&ye(E.customSounds.map((V,J)=>({id:"custom_"+J,name:V.name||V,icon:"🎼",desc:"自定义"}))),E.aiKeys&&I(E.aiKeys),E.aiModels&&R(V=>({...V,...E.aiModels})),E.defaultModel&&U(E.defaultModel),E.exKeys&&$(V=>({...V,...E.exKeys})))})},[]);function De(E,V="ok"){c({msg:E,type:V}),setTimeout(()=>c({msg:"",type:"ok"}),2800)}async function Fe(E,V="已保存"){h(!0);const J=await Bg(E);h(!1),J.status==="success"?De(V):De(J.msg||"保存失败","err")}async function $n(E,V={}){h(!0);const J=await Fg({channel:E,...V});h(!1),De(J.msg,J.status==="success"?"ok":"err")}async function Hn(){if(!fe||!we)return De("请填写所有密码字段","err");if(we!==ft)return De("两次新密码不一致","err");if(we.length<8)return De("新密码至少 8 位","err");h(!0);const E=await Ug(fe,we);h(!1),De(E.msg,E.status==="success"?"ok":"err"),E.status==="success"&&(Ge(""),Wn(""),St(""))}function Fr(E){const V=E.target.files[0];if(!V)return;if(!V.type.startsWith("audio/")){De("请上传音频文件（MP3/WAV/OGG）","err");return}const J=V.name.replace(/\.[^.]+$/,"");ye(cn=>[...cn,{id:"custom_"+Date.now(),name:J,icon:"🎼",desc:"自定义"}]),De(`已添加提示音：${J}`)}const Kn=[...new Set(uc.map(E=>E.group))],ln=!!Ko();return r.jsxs("div",{className:"sp-page",children:[r.jsx("style",{children:Wg}),r.jsx(sn,{activePage:e,onNavigate:t,currentUser:n,onOpenLogin:o,onLogout:i}),r.jsxs("div",{className:"sp-body",children:[r.jsx("div",{className:"sp-sidebar",children:Kn.map(E=>r.jsxs("div",{children:[r.jsx("div",{className:"sp-sidebar-title",children:E}),uc.filter(V=>V.group===E).map(V=>r.jsxs("button",{className:`sp-nav-btn ${a===V.id?"active":""}`,onClick:()=>s(V.id),children:[V.icon,V.label]},V.id))]},E))}),r.jsxs("div",{className:"sp-main",children:[a==="notifications"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sp-section-title",children:"通知渠道"}),r.jsxs("div",{className:"sp-section-sub",children:["配置信号触发时的推送方式，支持多渠道同时推送。",!ln&&r.jsx("span",{style:{color:"#F6465D"},children:" 需要登录后才能保存。"})]}),r.jsxs("div",{className:"sp-card",children:[r.jsx("div",{className:"sp-card-title",children:"邮件推送"}),r.jsx("div",{className:"sp-card-desc",children:"绑定邮箱后，信号触发时自动发送提醒邮件。邮件 SMTP 账号在交易所设置中配置。"}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"接收邮箱"}),r.jsx("input",{className:"sp-input",value:g,onChange:E=>y(E.target.value),placeholder:"your@email.com"})]}),r.jsx("div",{style:{display:"flex",justifyContent:"flex-end",marginTop:16},children:r.jsx("button",{className:"sp-btn sp-btn-primary",disabled:d,onClick:()=>Fe({email:g},"邮箱已保存"),children:"保存"})})]}),r.jsxs("div",{className:"sp-card",children:[r.jsxs("div",{style:{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:4},children:[r.jsx("div",{className:"sp-card-title",children:"Telegram 推送"}),r.jsx(so,{checked:u,onChange:m})]}),r.jsx("div",{className:"sp-card-desc",children:"通过 Telegram Bot 接收实时信号推送。需先在 @BotFather 创建机器人。"}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"Bot Token"}),r.jsx("input",{className:"sp-input",value:v,onChange:E=>b(E.target.value),placeholder:"123456:ABC-DEF...",disabled:!u})]}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"Chat ID"}),r.jsx("input",{className:"sp-input",value:z,onChange:E=>p(E.target.value),placeholder:"-100123456789",disabled:!u})]}),r.jsxs("div",{style:{display:"flex",gap:10,justifyContent:"flex-end",marginTop:16},children:[r.jsx("button",{className:"sp-btn sp-btn-test sp-btn-sm",disabled:!u||d,onClick:()=>$n("tg",{tgToken:v,tgChatId:z}),children:"测试推送"}),r.jsx("button",{className:"sp-btn sp-btn-primary",disabled:d,onClick:()=>Fe({tgToken:u?v:"",tgChatId:u?z:""},"Telegram 配置已保存"),children:"保存"})]})]}),r.jsxs("div",{className:"sp-card",children:[r.jsxs("div",{style:{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:4},children:[r.jsx("div",{className:"sp-card-title",children:"Discord 推送"}),r.jsx(so,{checked:w,onChange:C})]}),r.jsx("div",{className:"sp-card-desc",children:"通过 Discord Webhook 将信号推送到指定频道。"}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"Webhook URL"}),r.jsx("input",{className:"sp-input",value:j,onChange:E=>T(E.target.value),placeholder:"https://discord.com/api/webhooks/...",disabled:!w})]}),r.jsxs("div",{style:{display:"flex",gap:10,justifyContent:"flex-end",marginTop:16},children:[r.jsx("button",{className:"sp-btn sp-btn-test sp-btn-sm",disabled:!w||d,onClick:()=>$n("discord",{discordUrl:j}),children:"测试推送"}),r.jsx("button",{className:"sp-btn sp-btn-primary",disabled:d,onClick:()=>Fe({discordUrl:w?j:""},"Discord 配置已保存"),children:"保存"})]})]})]}),a==="webhook"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sp-section-title",children:"Webhook 集成"}),r.jsx("div",{className:"sp-section-sub",children:"信号触发时向自定义端点发送 POST 请求。"}),r.jsxs("div",{className:"sp-card",children:[r.jsxs("div",{style:{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:12},children:[r.jsx("div",{className:"sp-card-title",children:"主 Webhook"}),r.jsx(so,{checked:N,onChange:B})]}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"端点 URL"}),r.jsx("input",{className:"sp-input",value:_,onChange:E=>P(E.target.value),placeholder:"https://your-server.com/webhook"})]}),r.jsxs("div",{style:{display:"flex",gap:10,justifyContent:"space-between",marginTop:14},children:[r.jsx("button",{className:"sp-btn sp-btn-test sp-btn-sm",disabled:!N||!_||d,onClick:()=>$n("webhook",{webhookUrl:_}),children:"测试"}),r.jsx("button",{className:"sp-btn sp-btn-primary sp-btn-sm",disabled:d,onClick:()=>Fe({webhookUrl:N?_:""},"Webhook 已保存"),children:"保存"})]}),r.jsx("hr",{className:"sp-divider",style:{marginBottom:0}}),r.jsxs("div",{style:{marginTop:12,fontSize:"0.82rem",color:"var(--text-secondary)"},children:[r.jsx("div",{style:{marginBottom:6,fontWeight:600,color:"var(--text-primary)"},children:"Payload 格式"}),r.jsx("pre",{style:{background:"var(--input-bg)",padding:"10px 14px",borderRadius:8,overflow:"auto",fontSize:"0.78rem",lineHeight:1.6},children:`{
  "symbol": "BTCUSDT",
  "signal": "MACD_BULL",
  "price": 68420.5,
  "timestamp": 1716000000
}`})]})]})]}),a==="sound"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sp-section-title",children:"提示音"}),r.jsx("div",{className:"sp-section-sub",children:"选择信号触发时播放的提示音，或上传自定义音频文件。"}),r.jsxs("div",{className:"sp-card",children:[r.jsx("div",{className:"sp-card-title",children:"内置提示音"}),r.jsx("div",{className:"sp-card-desc",children:"点击卡片预览并选择"}),r.jsx("div",{className:"sp-sound-grid",children:[...$g,...Q].map(E=>r.jsxs("div",{className:`sp-sound-card ${K===E.id?"active":""}`,onClick:()=>{le(E.id),De(`已选择：${E.name}`)},children:[r.jsx("div",{className:"sp-sound-icon",children:E.icon}),r.jsx("div",{className:"sp-sound-name",children:E.name}),r.jsx("div",{className:"sp-sound-sub",children:E.desc})]},E.id))}),r.jsx("div",{style:{display:"flex",justifyContent:"flex-end"},children:r.jsx("button",{className:"sp-btn sp-btn-primary sp-btn-sm",disabled:d,onClick:()=>Fe({alertSettings:{sound_type:K}},"提示音已保存"),children:"保存选择"})}),r.jsx("hr",{className:"sp-divider"}),r.jsx("div",{className:"sp-card-title",style:{marginBottom:8},children:"上传自定义音频"}),r.jsxs("div",{className:"sp-upload-zone",onClick:()=>{var E;return(E=f.current)==null?void 0:E.click()},children:[r.jsxs("svg",{width:"28",height:"28",viewBox:"0 0 24 24",fill:"none",stroke:"var(--text-secondary)",strokeWidth:"1.5",strokeLinecap:"round",children:[r.jsx("path",{d:"M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"}),r.jsx("polyline",{points:"17 8 12 3 7 8"}),r.jsx("line",{x1:"12",y1:"3",x2:"12",y2:"15"})]}),r.jsx("div",{className:"sp-upload-text",children:"点击上传音频文件"}),r.jsx("div",{className:"sp-upload-hint",children:"支持 MP3、WAV、OGG，最大 2MB"})]}),r.jsx("input",{ref:f,type:"file",accept:"audio/*",style:{display:"none"},onChange:Fr})]})]}),a==="ai"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sp-section-title",children:"AI 大模型接口"}),r.jsx("div",{className:"sp-section-sub",children:"配置指标开发 AI 助手使用的模型，API Key 加密存储于服务器。"}),r.jsxs("div",{className:"sp-card",children:[r.jsx("div",{className:"sp-card-title",children:"默认模型"}),r.jsx("div",{className:"sp-card-desc",children:"指标生成 AI 优先使用的模型（需已配置对应 API Key）"}),r.jsxs("select",{className:"sp-select",style:{maxWidth:280},value:M,onChange:E=>U(E.target.value),children:[pc.filter(E=>E.id!=="custom").map(E=>r.jsxs("option",{value:E.id,children:[E.icon," ",E.name]},E.id)),r.jsx("option",{value:"custom",children:"⚙️ 自定义端点"})]}),r.jsx("div",{style:{display:"flex",justifyContent:"flex-end",marginTop:12},children:r.jsx("button",{className:"sp-btn sp-btn-primary sp-btn-sm",disabled:d,onClick:()=>Fe({defaultModel:M},"默认模型已保存"),children:"保存"})})]}),pc.map(E=>r.jsxs("div",{className:"sp-card",children:[r.jsxs("div",{className:"sp-model-row",style:{padding:0,border:"none"},children:[r.jsx("div",{className:"sp-model-icon",style:{background:E.bg},children:E.icon}),r.jsxs("div",{style:{flex:1},children:[r.jsx("div",{className:"sp-model-name",children:E.name}),E.id==="custom"&&r.jsx("div",{className:"sp-model-sub",children:"OpenAI 兼容端点（本地模型/代理）"})]}),r.jsxs("span",{className:`sp-badge ${pe[E.id]?"sp-badge-ok":"sp-badge-off"}`,children:[r.jsx("span",{className:"sp-dot"}),pe[E.id]?"已配置":"未配置"]})]}),r.jsxs("div",{className:"sp-row",style:{marginTop:14},children:[r.jsx("span",{className:"sp-label",children:E.id==="custom"?"Base URL":"API Key"}),r.jsx("input",{className:"sp-input",type:E.id==="custom"?"text":"password",placeholder:E.placeholder,value:pe[E.id]||"",onChange:V=>I(J=>({...J,[E.id]:V.target.value}))})]}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"模型版本"}),r.jsx("input",{className:"sp-input",placeholder:E.id==="custom"?"llama3-8b / qwen-72b ...":S[E.id],value:S[E.id]||"",onChange:V=>R(J=>({...J,[E.id]:V.target.value}))})]}),r.jsx("div",{style:{display:"flex",gap:10,justifyContent:"flex-end",marginTop:12},children:r.jsx("button",{className:"sp-btn sp-btn-primary sp-btn-sm",disabled:d,onClick:()=>Fe({aiKeys:pe,aiModels:S},`${E.name} 配置已保存`),children:"保存"})})]},E.id))]}),a==="exchange"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sp-section-title",children:"交易所 API"}),r.jsx("div",{className:"sp-section-sub",children:"绑定交易所 API Key，平台仅使用读取权限，不进行任何交易操作。"}),Hg.map(E=>{var V,J,cn,dn;return r.jsxs("div",{className:"sp-card",children:[r.jsxs("div",{style:{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:4},children:[r.jsxs("div",{className:"sp-card-title",children:[E.icon," ",E.name]}),r.jsxs("span",{className:`sp-badge ${(V=D[E.id])!=null&&V.key?"sp-badge-ok":"sp-badge-off"}`,children:[r.jsx("span",{className:"sp-dot"}),(J=D[E.id])!=null&&J.key?"已配置":"未配置"]})]}),r.jsx("div",{className:"sp-card-desc",children:"只需 Read 权限，无需提现 / 交易权限。"}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"API Key"}),r.jsx("input",{className:"sp-input",type:"password",placeholder:"粘贴 API Key...",value:((cn=D[E.id])==null?void 0:cn.key)||"",onChange:Vn=>$(Kt=>({...Kt,[E.id]:{...Kt[E.id],key:Vn.target.value}}))})]}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"Secret"}),r.jsx("input",{className:"sp-input",type:"password",placeholder:"粘贴 Secret...",value:((dn=D[E.id])==null?void 0:dn.secret)||"",onChange:Vn=>$(Kt=>({...Kt,[E.id]:{...Kt[E.id],secret:Vn.target.value}}))})]}),r.jsx("div",{style:{display:"flex",gap:10,justifyContent:"flex-end",marginTop:12},children:r.jsx("button",{className:"sp-btn sp-btn-primary sp-btn-sm",disabled:d,onClick:()=>Fe({exKeys:D},`${E.name} API 已保存`),children:"保存"})})]},E.id)})]}),a==="appearance"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sp-section-title",children:"外观偏好"}),r.jsx("div",{className:"sp-section-sub",children:"个性化界面语言、数据密度和动画效果。主题切换请使用顶部导航栏。"}),r.jsxs("div",{className:"sp-card",children:[r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"界面语言"}),r.jsxs("select",{className:"sp-select",value:F,onChange:E=>W(E.target.value),children:[r.jsx("option",{value:"zh",children:"简体中文"}),r.jsx("option",{value:"en",children:"English"}),r.jsx("option",{value:"tw",children:"繁體中文"})]})]}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"数据密度"}),r.jsxs("select",{className:"sp-select",value:Y,onChange:E=>te(E.target.value),children:[r.jsx("option",{value:"compact",children:"紧凑"}),r.jsx("option",{value:"normal",children:"标准"}),r.jsx("option",{value:"loose",children:"宽松"})]})]}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"过渡动画"}),r.jsx(so,{checked:Ee,onChange:Pe}),r.jsx("span",{style:{fontSize:"0.82rem",color:"var(--text-secondary)"},children:Ee?"开启（高端流畅）":"关闭（性能优先）"})]}),r.jsx("div",{style:{display:"flex",justifyContent:"flex-end",marginTop:16},children:r.jsx("button",{className:"sp-btn sp-btn-primary",disabled:d,onClick:()=>Fe({appearance:{lang:F,density:Y,animations:Ee}},"外观设置已保存"),children:"保存"})})]})]}),a==="account"&&r.jsxs(r.Fragment,{children:[r.jsx("div",{className:"sp-section-title",children:"账户安全"}),r.jsx("div",{className:"sp-section-sub",children:"管理登录密码及账户危险操作。"}),r.jsxs("div",{className:"sp-card",children:[r.jsx("div",{className:"sp-card-title",children:"修改密码"}),r.jsx("div",{className:"sp-card-desc",children:"建议使用至少 12 位包含大小写、数字和符号的密码。"}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"当前密码"}),r.jsx("input",{className:"sp-input",type:"password",value:fe,onChange:E=>Ge(E.target.value),placeholder:"••••••••"})]}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"新密码"}),r.jsx("input",{className:"sp-input",type:"password",value:we,onChange:E=>Wn(E.target.value),placeholder:"至少 8 位"})]}),r.jsxs("div",{className:"sp-row",children:[r.jsx("span",{className:"sp-label",children:"确认新密码"}),r.jsx("input",{className:"sp-input",type:"password",value:ft,onChange:E=>St(E.target.value),placeholder:"再次输入新密码"})]}),!ln&&r.jsx("div",{style:{fontSize:"0.82rem",color:"#F6465D",marginTop:6},children:"请先登录后再修改密码"}),r.jsx("div",{style:{display:"flex",justifyContent:"flex-end",marginTop:16},children:r.jsx("button",{className:"sp-btn sp-btn-primary",disabled:d||!ln,onClick:Hn,children:d?"更新中...":"更新密码"})})]}),r.jsxs("div",{className:"sp-card",style:{borderColor:"rgba(246,70,93,0.25)"},children:[r.jsx("div",{className:"sp-card-title",style:{color:"#F6465D"},children:"危险操作"}),r.jsx("div",{className:"sp-card-desc",children:"以下操作不可撤销，请谨慎操作。"}),r.jsxs("div",{style:{display:"flex",gap:12,flexWrap:"wrap"},children:[r.jsx("button",{className:"sp-btn sp-btn-danger",onClick:()=>{localStorage.clear(),De("已清除所有本地数据")},children:"清除本地数据"}),r.jsx("button",{className:"sp-btn sp-btn-danger",onClick:()=>De("账户注销请联系管理员","err"),children:"注销账户"})]})]})]})]})]}),l.msg&&r.jsx("div",{className:`sp-toast sp-toast-${l.type}`,children:l.msg})]})}const fc=["landing","premium","settings"];function Vg(){const[e,t]=x.useState("landing"),[n,o]=x.useState(!1),[i,a]=x.useState(null),[s,l]=x.useState(null),[c,d]=x.useState(()=>localStorage.getItem("ikun_exchange")||"binance");function h(b){d(b),localStorage.setItem("ikun_exchange",b)}x.useEffect(()=>{const b=localStorage.getItem("ikun_token");b&&fetch("/api/auth/check",{method:"POST",headers:{"Content-Type":"application/json",Authorization:`Bearer ${b}`,"X-Token":b},body:JSON.stringify({token:b})}).then(z=>z.json()).then(z=>{if(z.status==="ok"||z.status==="success"){const p=z.user||{email:z.email,nickname:z.nickname,role:"user"};a(p),t("monitor")}else localStorage.removeItem("ikun_token")}).catch(()=>{const z=localStorage.getItem("ikun_mock_user");if(z)try{a(JSON.parse(z)),t("monitor")}catch{}else localStorage.removeItem("ikun_token")})},[]);function f(b){a(b),o(!1),t("monitor")}function g(){const b=localStorage.getItem("ikun_token");b&&fetch("/api/auth/logout",{method:"POST",headers:{"Content-Type":"application/json","X-Token":b},body:JSON.stringify({token:b})}).catch(()=>{}),localStorage.removeItem("ikun_token"),localStorage.removeItem("ikun_mock_user"),a(null),t("landing")}function y(b){if(!i&&!fc.includes(b)){o(!0);return}t(b)}const v={currentUser:i,onNavigate:y,onOpenLogin:()=>o(!0),onLogout:g,currentExchange:c,onExchangeChange:h};return r.jsxs(r.Fragment,{children:[n&&r.jsx(Om,{onOpenLogin:()=>o(!1),onSuccess:f}),e==="landing"&&r.jsx(Pm,{onOpenLogin:()=>o(!0),onEnter:y,currentUser:i}),e==="monitor"&&r.jsx(dg,{...v,doLogout:g,activePage:e,activeIndicator:s,onClearIndicator:()=>l(null),externalExchange:c}),e==="signals"&&r.jsx(gg,{...v,activePage:e}),e==="market"&&r.jsx(xg,{...v,activePage:e}),e==="community"&&r.jsx(wg,{...v,activePage:e}),e==="indicators"&&r.jsx(Tg,{...v,activePage:e,onApplyIndicator:b=>{l(b),y("monitor")}}),e==="premium"&&r.jsx(Ag,{...v,activePage:e}),e==="settings"&&r.jsx(Kg,{...v,activePage:e}),!i&&!fc.includes(e)&&e!=="landing"&&r.jsxs("div",{style:{position:"fixed",inset:0,background:"rgba(3,8,18,0.97)",color:"#e2eaf4",display:"flex",alignItems:"center",justifyContent:"center",zIndex:99999,flexDirection:"column",gap:20,fontFamily:"'DM Sans', sans-serif"},children:[r.jsx("div",{style:{fontFamily:"'Rajdhani', sans-serif",fontSize:22,fontWeight:700},children:"请先登录"}),r.jsx("button",{onClick:()=>o(!0),style:{padding:"10px 32px",background:"#4db8ff",border:"none",borderRadius:8,cursor:"pointer",fontSize:15,fontWeight:700,color:"#030812",fontFamily:"'Rajdhani', sans-serif"},children:"登录 / 注册"}),r.jsx("button",{onClick:()=>t("landing"),style:{background:"none",border:"none",color:"rgba(255,255,255,0.35)",cursor:"pointer",fontSize:13},children:"返回首页"})]})]})}Wi.createRoot(document.getElementById("root")).render(r.jsx(Vg,{}));
